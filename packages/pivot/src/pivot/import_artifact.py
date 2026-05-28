from __future__ import annotations

import asyncio
import functools
import logging
import pathlib
import posixpath
import shutil
import tempfile
from typing import TYPE_CHECKING, NotRequired, TypedDict, cast

import aiohttp
import yaml

from pivot import config as pivot_config
from pivot import exceptions, project
from pivot.remote import config as remote_config
from pivot.remote import git_archive, github
from pivot.remote import storage as remote_storage
from pivot.storage import cache, track
from pivot.storage import lock as storage_lock

if TYPE_CHECKING:
    from collections.abc import Callable

    from pivot.types import DirManifestEntry, OutEntry, StorageLockData

logger = logging.getLogger(__name__)


class ResolvedImport(TypedDict):
    path: str
    hash: str
    size: int
    remote_url: str
    rev_lock: str
    stage: NotRequired[str]  # omitted for `pivot track`-ed source files
    # Set for directory imports; omitted for single-file imports.
    manifest: NotRequired[list[DirManifestEntry]]


class ImportResult(TypedDict):
    pvt_path: str
    data_path: str
    downloaded: bool


def _maybe_token(repo_url: str, token: str | None) -> str | None:
    if token is not None:
        return token
    if github.is_github_url(repo_url):
        return github.get_token()
    return None


async def _run_blocking[T](func: Callable[..., T], *args: object) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args))


async def _read_remote_file(
    repo_url: str,
    path: str,
    rev: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None = None,
) -> bytes | None:
    if github.is_github_url(repo_url):
        owner, repo = github.parse_github_url(repo_url)
        return await github.read_file(
            owner, repo, path, rev, _maybe_token(repo_url, token), session=session
        )
    return await _run_blocking(git_archive.read_file_from_remote_repo, repo_url, path, rev)


async def _resolve_ref(
    repo_url: str,
    ref: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None = None,
) -> str | None:
    if github.is_github_url(repo_url):
        owner, repo = github.parse_github_url(repo_url)
        return await github.resolve_ref(
            owner, repo, ref, _maybe_token(repo_url, token), session=session
        )
    return await _run_blocking(git_archive.resolve_ref_from_remote_repo, repo_url, ref)


async def read_remote_config(
    repo_url: str,
    rev: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None = None,
) -> str:
    raw = await _read_remote_file(repo_url, ".pivot/config.yaml", rev, token, session=session)
    if raw is None:
        raise exceptions.RemoteError("Remote repo is missing .pivot/config.yaml")

    try:
        data = cast("object", yaml.safe_load(raw))
    except yaml.YAMLError as e:
        raise exceptions.RemoteError(f"Invalid YAML in remote config: {e}") from e

    if not isinstance(data, dict):
        raise exceptions.RemoteError("Remote config is not a mapping")

    config_data = cast("dict[str, object]", data)
    remotes_raw = config_data.get("remotes")
    if not isinstance(remotes_raw, dict) or not remotes_raw:
        raise exceptions.RemoteError("Remote config has no remotes")

    remotes_data = cast("dict[object, object]", remotes_raw)
    remotes = {str(k): str(v) for k, v in remotes_data.items()}

    default_remote = config_data.get("default_remote")
    if not isinstance(default_remote, str):
        default_remote = None
    if default_remote is None:
        if len(remotes) == 1:
            default_remote = next(iter(remotes.keys()))
        else:
            raise exceptions.RemoteError("Remote config has multiple remotes but no default_remote")

    if default_remote not in remotes:
        raise exceptions.RemoteError(f"Default remote '{default_remote}' not found in config")

    url = str(remotes[default_remote])
    _ = remote_config.validate_s3_url(url)
    logger.info("Using source remote: %s", url)
    return url


async def list_remote_lock_files(
    repo_url: str,
    rev: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None = None,
) -> list[str]:
    if github.is_github_url(repo_url):
        owner, repo = github.parse_github_url(repo_url)
        entries = await github.list_directory(
            owner,
            repo,
            ".pivot/stages",
            rev,
            _maybe_token(repo_url, token),
            session=session,
        )
    else:
        entries = await _run_blocking(
            git_archive.list_directory_from_remote_repo,
            repo_url,
            ".pivot/stages",
            rev,
        )

    # Missing .pivot/stages or no lock files is fine — the repo may only use
    # `pivot track`, in which case resolution falls back to .pvt files.
    if entries is None:
        return []

    return [
        pathlib.Path(str(e)).name[: -len(".lock")]
        for e in entries
        if pathlib.Path(str(e)).name.endswith(".lock")
    ]


async def read_remote_lock_file(
    repo_url: str,
    stage_name: str,
    rev: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None = None,
) -> StorageLockData | None:
    path = f".pivot/stages/{stage_name}.lock"
    raw = await _read_remote_file(repo_url, path, rev, token, session=session)
    if raw is None:
        return None
    try:
        data = cast("object", yaml.safe_load(raw))
    except yaml.YAMLError:
        logger.warning("Failed to parse lock file for stage '%s'", stage_name)
        return None
    if not storage_lock.is_lock_data(data):
        logger.warning("Invalid lock file structure for stage '%s'", stage_name)
        return None
    return data


def _entry_size(entry: OutEntry) -> int:
    if "size" in entry:
        return int(entry["size"])
    if "manifest" in entry:
        return sum(int(m["size"]) for m in entry["manifest"])
    entry_path = entry["path"] if "path" in entry else "unknown"
    logger.warning("Lock entry '%s' has no size information", entry_path)
    return 0


def _iter_manifest_paths(entry: OutEntry) -> list[tuple[str, DirManifestEntry]]:
    if "manifest" not in entry:
        return []
    base = entry["path"].rstrip("/")
    return [(f"{base}/{m['relpath']}", m) for m in entry["manifest"]]


async def resolve_remote_path(
    repo_url: str,
    path: str,
    rev: str,
    token: str | None,
) -> ResolvedImport:
    async with aiohttp.ClientSession() as session:
        return await _resolve_remote_path_with_session(repo_url, path, rev, token, session)


class _Match(TypedDict):
    source: str  # Human-readable origin: stage name, or ".pvt file: <path>"
    stage: NotRequired[str]  # Stage name when match came from a lock file
    path: str
    hash: str
    size: int
    # Set for directory matches; absent for single-file matches.
    manifest: NotRequired[list[DirManifestEntry]]


async def _list_remote_pvt_files(
    repo_url: str,
    rev: str,
    token: str | None,
    *,
    session: aiohttp.ClientSession | None,
) -> dict[str, bytes]:
    """Return ``{pvt_path: bytes}`` for every ``*.pvt`` file in the remote tree at *rev*."""
    if github.is_github_url(repo_url):
        owner, repo = github.parse_github_url(repo_url)
        paths = await github.list_tree(
            owner, repo, rev, _maybe_token(repo_url, token), session=session
        )
        if not paths:
            return {}
        pvt_paths = [p for p in paths if p.endswith(".pvt")]
        contents = await asyncio.gather(
            *[
                github.read_file(
                    owner, repo, p, rev, _maybe_token(repo_url, token), session=session
                )
                for p in pvt_paths
            ]
        )
        return {p: c for p, c in zip(pvt_paths, contents, strict=True) if c is not None}
    result = await _run_blocking(git_archive.fetch_pvt_files_from_remote_repo, repo_url, rev)
    return result or {}


def _pvt_data_path(pvt_path: str, data_relpath: str) -> str:
    return posixpath.normpath(posixpath.join(posixpath.dirname(pvt_path), data_relpath))


def _collect_pvt_matches(
    pvt_files: dict[str, bytes],
    requested: str,
    available: set[str],
) -> list[_Match]:
    matches: list[_Match] = []
    for pvt_path, raw in pvt_files.items():
        try:
            data = cast("object", yaml.safe_load(raw))
        except yaml.YAMLError:
            logger.warning("Failed to parse remote .pvt file '%s'", pvt_path)
            continue
        if not track.is_pvt_data(data):
            logger.warning("Invalid .pvt structure at '%s'", pvt_path)
            continue
        # Skip import-of-import to avoid recursive resolution; users should import
        # from the original source repo directly.
        if track.is_import(data):
            continue

        data_full = _pvt_data_path(pvt_path, data["path"])
        available.add(data_full)
        manifest = data.get("manifest")

        if manifest is not None:
            for entry in manifest:
                full = posixpath.normpath(posixpath.join(data_full, entry["relpath"]))
                available.add(full)

        if data_full == requested:
            if manifest is not None:
                matches.append(
                    _Match(
                        source=f".pvt file: {pvt_path}",
                        path=requested,
                        hash=data["hash"],
                        size=int(data["size"]),
                        manifest=list(manifest),
                    )
                )
                continue
            matches.append(
                _Match(
                    source=f".pvt file: {pvt_path}",
                    path=requested,
                    hash=data["hash"],
                    size=int(data["size"]),
                )
            )
            continue

        if manifest is not None:
            for entry in manifest:
                full = posixpath.normpath(posixpath.join(data_full, entry["relpath"]))
                if full == requested:
                    matches.append(
                        _Match(
                            source=f".pvt file: {pvt_path}",
                            path=requested,
                            hash=entry["hash"],
                            size=int(entry["size"]),
                        )
                    )
    return matches


async def _resolve_remote_path_with_session(
    repo_url: str,
    path: str,
    rev: str,
    token: str | None,
    session: aiohttp.ClientSession,
) -> ResolvedImport:
    gh_session = session if github.is_github_url(repo_url) else None
    rev_lock = await _resolve_ref(repo_url, rev, token, session=gh_session)
    if rev_lock is None:
        raise exceptions.RemoteError(f"Unable to resolve ref '{rev}'")

    remote_url = await read_remote_config(repo_url, rev_lock, token, session=gh_session)

    # Use resolved SHA for all subsequent reads to avoid TOCTOU race.
    # Fetch lock files and .pvt-tracked files in parallel.
    stage_names, pvt_files = await asyncio.gather(
        list_remote_lock_files(repo_url, rev_lock, token, session=gh_session),
        _list_remote_pvt_files(repo_url, rev_lock, token, session=gh_session),
    )
    lock_datas = await asyncio.gather(
        *[
            read_remote_lock_file(repo_url, stage, rev_lock, token, session=gh_session)
            for stage in stage_names
        ]
    )

    matches: list[_Match] = []
    available = set[str]()

    for stage, lock_data in zip(stage_names, lock_datas, strict=True):
        if lock_data is None:
            continue
        for entry in lock_data["outs"]:
            available.add(entry["path"])
            for full_path, _ in _iter_manifest_paths(entry):
                available.add(full_path)

            if entry["path"] == path:
                if "manifest" in entry:
                    matches.append(
                        _Match(
                            source=stage,
                            stage=stage,
                            path=path,
                            hash=entry["hash"],
                            size=_entry_size(entry),
                            manifest=list(entry["manifest"]),
                        )
                    )
                    continue
                matches.append(
                    _Match(
                        source=stage,
                        stage=stage,
                        path=path,
                        hash=entry["hash"],
                        size=_entry_size(entry),
                    )
                )
                continue

            for full_path, manifest_entry in _iter_manifest_paths(entry):
                if full_path == path:
                    matches.append(
                        _Match(
                            source=stage,
                            stage=stage,
                            path=path,
                            hash=manifest_entry["hash"],
                            size=int(manifest_entry["size"]),
                        )
                    )

    matches.extend(_collect_pvt_matches(pvt_files, path, available))

    if not matches:
        max_shown = 10
        sorted_available = sorted(available)
        if len(sorted_available) > max_shown:
            shown = ", ".join(sorted_available[:max_shown])
            available_list = f"{shown} (and {len(sorted_available) - max_shown} more)"
        elif sorted_available:
            available_list = ", ".join(sorted_available)
        else:
            available_list = "(none)"
        raise exceptions.PivotError(
            f"Path '{path}' not found in remote outputs. Available outputs: {available_list}"
        )

    if len(matches) > 1:
        sources = ", ".join(sorted({m["source"] for m in matches}))
        raise exceptions.PivotError(f"Path '{path}' is produced by multiple sources: {sources}")

    match = matches[0]
    resolved = ResolvedImport(
        path=match["path"],
        hash=match["hash"],
        size=match["size"],
        remote_url=remote_url,
        rev_lock=rev_lock,
    )
    if "stage" in match:
        resolved["stage"] = match["stage"]
    if "manifest" in match:
        resolved["manifest"] = match["manifest"]
    return resolved


def _resolve_cache_dir(cache_dir: pathlib.Path | None) -> pathlib.Path:
    if cache_dir is not None:
        return cache_dir
    return pivot_config.get_cache_dir() / "files"


def _normalize_source_path(p: str) -> str:
    return posixpath.normpath(p).strip("/")


def _paths_overlap(a: str, b: str) -> bool:
    """True if a == b, or one path is a segment-aligned prefix of the other."""
    if a == b:
        return True
    a_parts = a.split("/")
    b_parts = b.split("/")
    if len(a_parts) < len(b_parts):
        return b_parts[: len(a_parts)] == a_parts
    return a_parts[: len(b_parts)] == b_parts


def _check_import_conflicts(
    *,
    repo_url: str,
    source_path: str,
    project_root: pathlib.Path,
    exclude_pvt: pathlib.Path | None = None,
) -> None:
    """Reject if any existing import .pvt overlaps source_path from the same repo.

    Overlap means either equal paths or one is a directory prefix of the other
    (e.g., importing 'data/raw/' when 'data/raw/foo.csv' is already imported, or vice
    versa). Detection is at import time; pvt_status etc. are not affected.
    """
    existing = track.discover_import_pvt_files(project_root)
    new_norm = _normalize_source_path(source_path)
    exclude_abs = exclude_pvt.absolute() if exclude_pvt is not None else None

    for data_path_str, pvt_data in existing.items():
        existing_pvt_path = track.get_pvt_path(pathlib.Path(data_path_str)).absolute()
        if exclude_abs is not None and existing_pvt_path == exclude_abs:
            continue
        existing_source = pvt_data.get("source")
        if existing_source is None or existing_source["repo"] != repo_url:
            continue
        existing_norm = _normalize_source_path(existing_source["path"])
        if _paths_overlap(new_norm, existing_norm):
            raise exceptions.PivotError(
                f"Import conflict: '{source_path}' from {repo_url} overlaps with "
                + f"existing import at '{existing_pvt_path}' (source path "
                + f"'{existing_source['path']}'). A directory import and a file "
                + "import from the same source repo cannot cover overlapping paths."
            )


async def _materialize_file(
    *, remote_url: str, file_hash: str, dest: pathlib.Path, cache_dir: pathlib.Path
) -> None:
    """Materialize a single file into ``dest`` from cache (hit) or remote (miss).

    On miss: download, verify hash, populate cache. On hit: copy from cache.
    """
    cache_path = cache.get_cache_path(cache_dir, file_hash)
    if cache_path.exists():
        shutil.copyfile(cache_path, dest)
        return

    remote = remote_storage.S3Remote(remote_url)
    await remote.download_file(file_hash, dest)
    actual_hash, _ = cache.hash_file(dest)
    if actual_hash != file_hash:
        dest.unlink(missing_ok=True)
        raise exceptions.RemoteError(
            f"Downloaded file hash mismatch: expected {file_hash}, got {actual_hash}"
        )
    try:
        cache.copy_to_cache(dest, cache_path)
    except OSError as exc:
        logger.warning("Failed to populate cache for %s: %s", file_hash, exc)


async def _materialize_directory(
    *, resolved: ResolvedImport, data_path: pathlib.Path, cache_dir: pathlib.Path
) -> None:
    """Materialize a directory import: stage all files, then atomically swap into place.

    Files already present in the local cache (by content hash) are copied from cache
    instead of downloaded — this gives delta updates "for free" when only a subset
    of the upstream manifest has changed.

    Atomicity: on any per-file failure the staging directory is removed and no
    changes are made to ``data_path``. Hash mismatches never retry (corruption
    signal); transient S3 errors are retried by the S3 client itself.
    """
    assert "manifest" in resolved, "_materialize_directory called on a non-directory import"
    manifest = resolved["manifest"]

    data_path.parent.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(
        tempfile.mkdtemp(prefix=f".{data_path.name}.import-", dir=data_path.parent)
    )

    try:
        download_items: list[tuple[str, pathlib.Path]] = []
        cache_items: list[tuple[pathlib.Path, pathlib.Path]] = []
        for entry in manifest:
            staged = staging / entry["relpath"]
            staged.parent.mkdir(parents=True, exist_ok=True)
            cache_path = cache.get_cache_path(cache_dir, entry["hash"])
            if cache_path.exists():
                cache_items.append((cache_path, staged))
            else:
                download_items.append((entry["hash"], staged))

        for cache_path, staged in cache_items:
            shutil.copyfile(cache_path, staged)

        if download_items:
            remote = remote_storage.S3Remote(resolved["remote_url"])
            results = await remote.download_batch(download_items)
            failures = [r for r in results if not r["success"]]
            if failures:
                err_msgs = "; ".join(r["error"] for r in failures if "error" in r)
                raise exceptions.RemoteError(
                    f"Failed to download {len(failures)} of {len(download_items)} "
                    + f"files for directory import: {err_msgs}"
                )

        # Verify hashes for downloaded files; cache hits are trusted by construction.
        for blob_hash, staged in download_items:
            actual_hash, _ = cache.hash_file(staged)
            if actual_hash != blob_hash:
                raise exceptions.RemoteError(
                    f"Hash mismatch for downloaded blob: expected {blob_hash}, "
                    + f"got {actual_hash}"
                )

        for entry in manifest:
            if entry["isexec"]:
                staged = staging / entry["relpath"]
                staged.chmod(staged.stat().st_mode | 0o111)

        for blob_hash, staged in download_items:
            cache_path = cache.get_cache_path(cache_dir, blob_hash)
            try:
                cache.copy_to_cache(staged, cache_path)
            except OSError as exc:
                logger.warning("Failed to populate cache for %s: %s", blob_hash, exc)

        if data_path.exists():
            shutil.rmtree(data_path)
        staging.rename(data_path)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _build_source(repo_url: str, rev: str, resolved: ResolvedImport) -> track.ImportSource:
    source = track.ImportSource(
        repo=repo_url,
        rev=rev,
        rev_lock=resolved["rev_lock"],
        path=resolved["path"],
        remote=resolved["remote_url"],
    )
    if "stage" in resolved:
        source["stage"] = resolved["stage"]
    return source


def _build_pvt_data(
    *,
    path_name: str,
    file_size: int,
    resolved: ResolvedImport,
    source: track.ImportSource,
) -> track.PvtData:
    pvt_data = track.PvtData(
        path=path_name,
        hash=resolved["hash"],
        size=file_size,
        source=source,
    )
    if "manifest" in resolved:
        manifest = resolved["manifest"]
        pvt_data["manifest"] = manifest
        pvt_data["num_files"] = len(manifest)
    return pvt_data


async def import_artifact(
    repo_url: str,
    path: str,
    *,
    rev: str = "main",
    out: str | None = None,
    force: bool = False,
    no_download: bool = False,
    project_root: pathlib.Path | None = None,
    cache_dir: pathlib.Path | None = None,
) -> ImportResult:
    resolved = await resolve_remote_path(repo_url, path, rev, None)

    if project_root is None:
        project_root = project.get_project_root()

    dest = out if out is not None else path
    data_path = project.normalize_path(dest, base=project_root)
    if not data_path.is_relative_to(project_root.absolute()):
        raise exceptions.PivotError(
            f"Import destination must be within project root. Got: {data_path}"
        )
    pvt_path = track.get_pvt_path(data_path)
    is_directory = "manifest" in resolved

    _check_import_conflicts(
        repo_url=repo_url,
        source_path=resolved["path"],
        project_root=project_root,
        exclude_pvt=pvt_path if force else None,
    )

    if data_path.exists() and not force:
        raise exceptions.PivotError(f"'{data_path}' already exists. Use --force to overwrite.")
    if pvt_path.exists() and not force:
        raise exceptions.PivotError(f"'{pvt_path}' already exists. Use --force to overwrite.")

    if force and data_path.exists():
        if data_path.is_dir() and not is_directory:
            raise exceptions.PivotError("Cannot overwrite directory with file import")
        if data_path.is_file() and is_directory:
            raise exceptions.PivotError("Cannot overwrite file with directory import")

    pvt_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.parent.mkdir(parents=True, exist_ok=True)

    downloaded = False
    file_size = resolved["size"]
    if not no_download:
        files_cache_dir = _resolve_cache_dir(cache_dir)
        if is_directory:
            await _materialize_directory(
                resolved=resolved, data_path=data_path, cache_dir=files_cache_dir
            )
        else:
            await _materialize_file(
                remote_url=resolved["remote_url"],
                file_hash=resolved["hash"],
                dest=data_path,
                cache_dir=files_cache_dir,
            )
            file_size = data_path.stat().st_size
        downloaded = True

    source = _build_source(repo_url, rev, resolved)
    pvt_data = _build_pvt_data(
        path_name=data_path.name,
        file_size=file_size,
        resolved=resolved,
        source=source,
    )

    track.write_pvt_file(pvt_path, pvt_data)

    return ImportResult(
        pvt_path=str(pvt_path),
        data_path=str(data_path),
        downloaded=downloaded,
    )


class UpdateCheck(TypedDict):
    available: bool
    current_rev: str
    latest_rev: str


class UpdateResult(TypedDict):
    downloaded: bool
    metadata_updated: bool
    updated: bool
    old_rev: str
    new_rev: str
    path: str


async def check_for_update(pvt_data: track.PvtData) -> UpdateCheck:
    """Check if import has updates. Resolves current ref, compares to rev_lock."""
    if "source" not in pvt_data:
        raise exceptions.PivotError("Not an import")
    source = pvt_data["source"]
    rev_lock = await _resolve_ref(source["repo"], source["rev"], None)
    if rev_lock is None:
        raise exceptions.RemoteError(f"Cannot resolve ref '{source['rev']}' from {source['repo']}")
    return UpdateCheck(
        available=rev_lock != source["rev_lock"],
        current_rev=source["rev_lock"],
        latest_rev=rev_lock,
    )


async def update_import(
    pvt_path: pathlib.Path,
    *,
    new_rev: str | None = None,
    cache_dir: pathlib.Path | None = None,
) -> UpdateResult:
    """Update an imported artifact. Re-resolves ref, re-downloads if hash changed."""
    pvt_data = track.read_pvt_file(pvt_path)
    if pvt_data is None:
        raise exceptions.PivotError(f"Invalid .pvt file: {pvt_path}")
    if "source" not in pvt_data:
        raise exceptions.PivotError(f"Not an import: {pvt_path}")

    source = pvt_data["source"]
    rev = new_rev if new_rev is not None else source["rev"]
    old_rev = source["rev_lock"]

    resolved = await resolve_remote_path(source["repo"], source["path"], rev, None)
    is_directory = "manifest" in resolved
    data_path = track.get_data_path(pvt_path)
    need_download = resolved["hash"] != pvt_data["hash"] or not data_path.exists()

    downloaded = False
    file_size = resolved["size"]
    if need_download:
        files_cache_dir = _resolve_cache_dir(cache_dir)
        if is_directory:
            await _materialize_directory(
                resolved=resolved, data_path=data_path, cache_dir=files_cache_dir
            )
        else:
            await _materialize_file(
                remote_url=resolved["remote_url"],
                file_hash=resolved["hash"],
                dest=data_path,
                cache_dir=files_cache_dir,
            )
            file_size = data_path.stat().st_size
        downloaded = True
    elif data_path.exists() and not is_directory:
        file_size = data_path.stat().st_size

    metadata_changed = (
        resolved["rev_lock"] != old_rev
        or resolved["hash"] != pvt_data["hash"]
        or rev != source["rev"]
    )
    if metadata_changed:
        new_source = _build_source(source["repo"], rev, resolved)
        new_pvt = _build_pvt_data(
            path_name=pvt_data["path"],
            file_size=file_size,
            resolved=resolved,
            source=new_source,
        )
        track.write_pvt_file(pvt_path, new_pvt)

    return UpdateResult(
        downloaded=downloaded,
        metadata_updated=metadata_changed,
        updated=downloaded,
        old_rev=old_rev,
        new_rev=resolved["rev_lock"],
        path=str(data_path),
    )
