from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from typing import TYPE_CHECKING

from pivot import config, exceptions, metrics, path_utils, project, registry
from pivot.remote import config as remote_config
from pivot.remote import storage as remote_mod
from pivot.storage import cache, lock, track
from pivot.types import DirHash, FileHash, HashInfo, RemoteStatus, TransferSummary, is_dir_hash

if TYPE_CHECKING:
    from collections.abc import Callable

    from pivot.registry import RegistryStageInfo
    from pivot.storage import state as state_mod

logger = logging.getLogger(__name__)


def get_local_cache_hashes(cache_dir: pathlib.Path) -> set[str]:
    """Scan local cache and return all content hashes.

    Uses os.scandir for efficiency - DirEntry caches stat results, eliminating
    redundant syscalls compared to pathlib.iterdir + is_file/is_dir.
    """
    _t = metrics.start()
    files_dir = cache_dir / "files"
    if not files_dir.exists():
        metrics.end("sync.get_local_cache_hashes", _t)
        return set()

    hashes = set[str]()
    with os.scandir(files_dir) as prefix_entries:
        for prefix_entry in prefix_entries:
            if not prefix_entry.is_dir() or len(prefix_entry.name) != 2:
                continue
            with os.scandir(prefix_entry.path) as hash_entries:
                for hash_entry in hash_entries:
                    if hash_entry.is_file():
                        full_hash = prefix_entry.name + hash_entry.name
                        if len(full_hash) == cache.XXHASH64_HEX_LENGTH:
                            hashes.add(full_hash)

    metrics.end("sync.get_local_cache_hashes", _t)
    return hashes


def _extract_file_hashes_from_hash_info(hash_info: HashInfo) -> set[str]:
    """Extract hashes that correspond to cached file blobs only.

    For directories, returns only manifest entry hashes (not the tree hash).
    """
    if is_dir_hash(hash_info):
        return {entry["hash"] for entry in hash_info["manifest"]}
    return {hash_info["hash"]}


def _expand_hash_info_paths(hash_info: HashInfo, base_path: str) -> dict[str, str]:
    """Map each blob hash in hash_info to a human-readable path.

    For directories, each manifest entry maps to ``base_path/relpath``.
    """
    if is_dir_hash(hash_info):
        return {entry["hash"]: f"{base_path}/{entry['relpath']}" for entry in hash_info["manifest"]}
    return {hash_info["hash"]: base_path}


def get_stage_output_hashes(state_dir: pathlib.Path, stage_names: list[str]) -> set[str]:
    """Extract output hashes from lock files for specific stages.

    Only includes hashes for outputs with cache=True.
    """
    hashes = set[str]()
    from pivot.cli import helpers as cli_helpers

    for stage_name in stage_names:
        try:
            stage_info = cli_helpers.get_stage(stage_name)
        except KeyError:
            logger.warning(f"Stage {stage_name} not found in registry, skipping")
            continue
        non_cached_paths = {str(out.path) for out in stage_info["outs"] if not out.cache}

        stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(state_dir))
        lock_data = stage_lock.read()
        if lock_data is None:
            logger.warning(f"No lock file for stage '{stage_name}'")
            continue

        for out_path, output_hash in lock_data["output_hashes"].items():
            if out_path not in non_cached_paths:
                hashes |= _extract_file_hashes_from_hash_info(output_hash)

    return hashes


def get_stage_dep_hashes(state_dir: pathlib.Path, stage_names: list[str]) -> set[str]:
    """Extract dependency hashes from lock files for specific stages."""
    hashes = set[str]()

    for stage_name in stage_names:
        stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(state_dir))
        lock_data = stage_lock.read()
        if lock_data is None:
            continue

        for dep_hash in lock_data["dep_hashes"].values():
            hashes |= _extract_file_hashes_from_hash_info(dep_hash)

    return hashes


def _get_file_hash_from_stages(
    abs_path: str,
    state_dir: pathlib.Path,
    all_stages: dict[str, RegistryStageInfo] | None = None,
) -> HashInfo | None:
    """Look up a file's hash from stage lock files."""
    if all_stages is not None:
        for stage_name, stage_info in all_stages.items():
            stage_state_dir = registry.get_stage_state_dir(stage_info, state_dir)
            stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(stage_state_dir))
            lock_data = stage_lock.read()
            if lock_data is None:
                continue
            non_cached_paths = {str(out.path) for out in stage_info["outs"] if not out.cache}
            for out_path, out_hash in lock_data["output_hashes"].items():
                if out_path == abs_path and out_path not in non_cached_paths:
                    return out_hash
        return None

    from pivot.cli import helpers as cli_helpers

    stages_dir = lock.get_stages_dir(state_dir)
    if not stages_dir.exists():
        return None

    for lock_file in stages_dir.rglob("*.lock"):
        stage_name = lock_file.relative_to(stages_dir).with_suffix("").as_posix()
        try:
            stage_info = cli_helpers.get_stage(stage_name)
            non_cached_paths = {str(out.path) for out in stage_info["outs"] if not out.cache}
        except (KeyError, exceptions.PivotError) as exc:
            logger.debug("Stage %s not available for cache filtering: %s", stage_name, exc)
            non_cached_paths = set[str]()

        stage_lock = lock.StageLock(stage_name, stages_dir)
        lock_data = stage_lock.read()
        if lock_data is None:
            continue

        for out_path, out_hash in lock_data["output_hashes"].items():
            if out_path == abs_path and out_path not in non_cached_paths:
                return out_hash

    return None


def _get_file_hash_from_pvt(rel_path: str, proj_root: pathlib.Path) -> HashInfo | None:
    """Look up a file's hash from .pvt tracking file."""
    pvt_path = proj_root / (rel_path + ".pvt")
    if not pvt_path.exists():
        return None

    track_data = track.read_pvt_file(pvt_path)
    if track_data is None:
        return None

    if "manifest" in track_data:
        return DirHash(hash=track_data["hash"], manifest=track_data["manifest"])
    return FileHash(hash=track_data["hash"])


def _get_cacheable_artifact_hashes(
    state_dir: pathlib.Path,
    all_stages: dict[str, RegistryStageInfo],
    proj_root: pathlib.Path,
) -> set[str]:
    """Blob hashes that can exist in the cache/remote: cached stage outputs plus
    .pvt-tracked artifacts.

    Raw-input dep contents are never written to the cache (only stage outputs
    are), so ``include_deps`` intersects a stage's dep hashes against this set to
    avoid fetching blobs that were never pushed.
    """
    hashes = set[str]()
    for stage_name, stage_info in all_stages.items():
        stage_state_dir = registry.get_stage_state_dir(stage_info, state_dir)
        try:
            stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(stage_state_dir))
        except ValueError:
            continue
        lock_data = stage_lock.read()
        if lock_data is None:
            continue
        non_cached_paths = {str(out.path) for out in stage_info["outs"] if not out.cache}
        for out_path, out_hash in lock_data["output_hashes"].items():
            if out_path not in non_cached_paths:
                hashes |= _extract_file_hashes_from_hash_info(out_hash)
    for pvt in track.discover_pvt_files(proj_root).values():
        hashes |= _extract_file_hashes_from_hash_info(track.pvt_to_hash_info(pvt))
    return hashes


def build_hash_path_index(
    state_dir: pathlib.Path,
    all_stages: dict[str, RegistryStageInfo] | None,
    proj_root: pathlib.Path,
) -> dict[str, str]:
    """Best-effort blob-hash -> project-relative path map for progress display.

    Walks all stage lock outputs + deps and all .pvt files. Intentionally broad (no
    target/exclude filtering): extra entries are harmless since callers only look up
    hashes they are actually downloading. Last-writer-wins on hash collisions across
    paths is fine (content-addressed: same hash == same bytes).
    """
    index = dict[str, str]()
    if all_stages:
        for stage_name, stage_info in all_stages.items():
            stage_state_dir = registry.get_stage_state_dir(stage_info, state_dir)
            try:
                stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(stage_state_dir))
            except ValueError:
                continue
            lock_data = stage_lock.read()
            if lock_data is None:
                continue
            for out_path, out_hash in lock_data["output_hashes"].items():
                rel = project.to_relative_path(out_path, proj_root)
                index.update(_expand_hash_info_paths(out_hash, rel))
            for dep_path, dep_hash in lock_data["dep_hashes"].items():
                rel = project.to_relative_path(dep_path, proj_root)
                index.update(_expand_hash_info_paths(dep_hash, rel))
    for data_path, pvt in track.discover_pvt_files(proj_root).items():
        rel = project.to_relative_path(data_path, proj_root)
        index.update(_expand_hash_info_paths(track.pvt_to_hash_info(pvt), rel))
    return index


def get_target_hashes(
    targets: list[str],
    state_dir: pathlib.Path,
    include_deps: bool = False,
    all_stages: dict[str, RegistryStageInfo] | None = None,
    exclude: Callable[[str], bool] | None = None,
) -> set[str]:
    """Resolve targets (stage names or file paths) to cache hashes."""
    _t = metrics.start()
    proj_root = project.get_project_root()
    hashes = set[str]()
    unresolved = list[str]()

    cacheable: set[str] | None = None
    if include_deps and all_stages is not None:
        cacheable = _get_cacheable_artifact_hashes(state_dir, all_stages, proj_root)

    for target in targets:
        is_known_stage = all_stages is not None and target in all_stages
        if is_known_stage:
            assert all_stages is not None
            stage_info = all_stages[target]
            target_state_dir = registry.get_stage_state_dir(stage_info, state_dir)
            non_cached_paths = {str(out.path) for out in stage_info["outs"] if not out.cache}

            try:
                stage_lock = lock.StageLock(target, lock.get_stages_dir(target_state_dir))
            except ValueError:
                stage_lock = None

            if stage_lock is not None:
                lock_data = stage_lock.read()
                if lock_data is not None:
                    for out_path, out_hash in lock_data["output_hashes"].items():
                        if out_path in non_cached_paths:
                            continue
                        if exclude and exclude(project.to_relative_path(out_path, proj_root)):
                            continue
                        hashes |= _extract_file_hashes_from_hash_info(out_hash)
                    if include_deps and cacheable is not None:
                        for dep_path, dep_hash in lock_data["dep_hashes"].items():
                            if exclude and exclude(project.to_relative_path(dep_path, proj_root)):
                                continue
                            hashes |= _extract_file_hashes_from_hash_info(dep_hash) & cacheable
                    continue

        # Strip .pvt suffix if present (CLI normalizes these, but be defensive)
        target_p = pathlib.Path(target)
        if target_p.suffix == ".pvt":
            target = str(track.get_data_path(target_p))

        abs_path = str(project.normalize_path(target))
        rel_path = project.to_relative_path(abs_path, proj_root)

        if exclude and exclude(rel_path):
            continue

        out_hash = _get_file_hash_from_stages(abs_path, state_dir, all_stages)
        if out_hash is not None:
            hashes |= _extract_file_hashes_from_hash_info(out_hash)
            continue

        pvt_hash = _get_file_hash_from_pvt(rel_path, proj_root)
        if pvt_hash is not None:
            hashes |= _extract_file_hashes_from_hash_info(pvt_hash)
            continue

        unresolved.append(target)

    if unresolved:
        logger.warning(f"Could not resolve targets: {', '.join(unresolved)}")

    metrics.end("sync.get_target_hashes", _t)
    return hashes


def get_referenced_hashes(
    state_dir: pathlib.Path,
    all_stages: dict[str, RegistryStageInfo] | None,
    proj_root: pathlib.Path,
    exclude: Callable[[str], bool] | None = None,
) -> set[str]:
    """Cache hashes referenced by the currently checked-out project.

    Includes every registered stage's cached outputs and dependency hashes, plus
    all .pvt-tracked files. Stale blobs left in the remote that are no longer
    referenced are intentionally excluded.
    """
    _t = metrics.start()
    hashes = set[str]()
    if all_stages:
        hashes |= get_target_hashes(
            list(all_stages), state_dir, include_deps=True, all_stages=all_stages, exclude=exclude
        )
    for data_path, pvt in track.discover_pvt_files(proj_root).items():
        if exclude and exclude(project.to_relative_path(data_path, proj_root)):
            continue
        hashes |= _extract_file_hashes_from_hash_info(track.pvt_to_hash_info(pvt))
    metrics.end("sync.get_referenced_hashes", _t)
    return hashes


def get_needed_hashes(
    targets: list[str] | None,
    state_dir: pathlib.Path,
    all_stages: dict[str, RegistryStageInfo] | None,
    proj_root: pathlib.Path,
    exclude_patterns: list[str] | None = None,
) -> set[str]:
    """Resolve which cache hashes pull/fetch should download.

    Single source of truth shared by the dry-run and execution paths: explicit
    targets resolve to their hashes, otherwise the full set of project-referenced
    hashes (not the entire remote bucket). Paths matching ``exclude_patterns``
    (project-relative directory prefixes or exact paths) are never accumulated.
    """
    exclude = path_utils.make_exclude_matcher(exclude_patterns or [])
    if targets:
        return get_target_hashes(
            targets, state_dir, include_deps=True, all_stages=all_stages, exclude=exclude
        )
    return get_referenced_hashes(state_dir, all_stages, proj_root, exclude=exclude)


def _check_remote_url(
    state_db: state_mod.StateDB,
    remote_name: str,
    remote: remote_mod.S3Remote,
) -> None:
    """Detect remote URL change and clear stale index."""
    current_url = remote.url
    stored_url = state_db.remote_get_url(remote_name)
    if stored_url is None:
        # First use — record URL, don't clear (fail-open)
        state_db.remote_set_url(remote_name, current_url)
    elif stored_url != current_url:
        logger.warning(
            "Remote '%s' URL changed (%s → %s), clearing cached index",
            remote_name,
            stored_url,
            current_url,
        )
        state_db.remote_index_clear(remote_name)
        state_db.remote_set_url(remote_name, current_url)
    else:
        logger.debug("Remote '%s' URL unchanged: %s", remote_name, current_url)


async def compare_status(
    local_hashes: set[str],
    remote: remote_mod.S3Remote,
    state_db: state_mod.StateDB,
    remote_name: str,
    jobs: int | None = None,
) -> RemoteStatus:
    """Compare local cache against remote, using index to minimize HEAD requests."""
    _t = metrics.start()
    if not local_hashes:
        metrics.end("sync.compare_status", _t)
        return RemoteStatus(local_only=set(), remote_only=set(), common=set())

    jobs = jobs if jobs is not None else config.get_remote_jobs()
    known_on_remote = state_db.remote_hashes_intersection(remote_name, local_hashes)
    unknown_hashes = local_hashes - known_on_remote

    if unknown_hashes:
        existence = await remote.bulk_exists(list(unknown_hashes), concurrency=jobs)
        newly_found = {h for h, exists in existence.items() if exists}
        state_db.remote_hashes_add(remote_name, newly_found)
        known_on_remote = known_on_remote | newly_found

    local_only = local_hashes - known_on_remote
    common = local_hashes & known_on_remote

    metrics.end("sync.compare_status", _t)
    return RemoteStatus(local_only=local_only, remote_only=set(), common=common)


async def _push_async(
    cache_dir: pathlib.Path,
    state_dir: pathlib.Path,
    remote: remote_mod.S3Remote,
    state_db: state_mod.StateDB,
    remote_name: str,
    targets: list[str] | None = None,
    jobs: int | None = None,
    callback: Callable[[float, int, str], None] | None = None,
    all_stages: dict[str, RegistryStageInfo] | None = None,
    byte_callback: Callable[[int], None] | None = None,
) -> TransferSummary:
    """Push cache files to remote (async implementation)."""
    _t = metrics.start()
    jobs = jobs if jobs is not None else config.get_remote_jobs()

    _check_remote_url(state_db, remote_name, remote)

    if targets:
        local_hashes = get_target_hashes(
            targets, state_dir, include_deps=False, all_stages=all_stages
        )
    else:
        local_hashes = get_local_cache_hashes(cache_dir)

    if not local_hashes:
        metrics.end("sync.push_async", _t)
        return TransferSummary(transferred=0, skipped=0, failed=0, errors=[])

    status = await compare_status(local_hashes, remote, state_db, remote_name, jobs)

    if not status["local_only"]:
        metrics.end("sync.push_async", _t)
        return TransferSummary(transferred=0, skipped=len(status["common"]), failed=0, errors=[])

    files_dir = cache_dir / "files"
    items = list[tuple[pathlib.Path, str]]()
    skipped_non_file = 0
    for hash_ in status["local_only"]:
        cache_path = cache.get_cache_path(files_dir, hash_)
        if cache_path.is_file():
            items.append((cache_path, hash_))
        else:
            skipped_non_file += 1
    if skipped_non_file:
        logger.debug("Skipped %d non-file cache entries during push", skipped_non_file)

    upload_callback = callback
    if callback is not None:
        name_by_hash = build_hash_path_index(state_dir, all_stages, project.get_project_root())

        def _translate(completed: float, total: int, ident: str) -> None:
            callback(completed, total, name_by_hash.get(ident, ident[:8]))

        upload_callback = _translate

    results = await remote.upload_batch(
        items, concurrency=jobs, callback=upload_callback, byte_callback=byte_callback
    )

    transferred = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    state_db.remote_hashes_add(remote_name, [r["hash"] for r in transferred])
    errors = [r["error"] for r in failed if "error" in r]

    metrics.end("sync.push_async", _t)
    return TransferSummary(
        transferred=len(transferred),
        skipped=len(status["common"]) + skipped_non_file,
        failed=len(failed),
        errors=errors,
    )


def push(
    cache_dir: pathlib.Path,
    state_dir: pathlib.Path,
    remote: remote_mod.S3Remote,
    state_db: state_mod.StateDB,
    remote_name: str,
    targets: list[str] | None = None,
    jobs: int | None = None,
    callback: Callable[[float, int, str], None] | None = None,
    all_stages: dict[str, RegistryStageInfo] | None = None,
    byte_callback: Callable[[int], None] | None = None,
) -> TransferSummary:
    """Push cache files to remote storage."""
    return asyncio.run(
        _push_async(
            cache_dir,
            state_dir,
            remote,
            state_db,
            remote_name,
            targets,
            jobs,
            callback,
            all_stages,
            byte_callback,
        )
    )


async def _pull_async(
    cache_dir: pathlib.Path,
    state_dir: pathlib.Path,
    remote: remote_mod.S3Remote,
    state_db: state_mod.StateDB,
    remote_name: str,
    targets: list[str] | None = None,
    jobs: int | None = None,
    callback: Callable[[float, int, str], None] | None = None,
    all_stages: dict[str, RegistryStageInfo] | None = None,
    exclude_patterns: list[str] | None = None,
    byte_callback: Callable[[int], None] | None = None,
) -> TransferSummary:
    """Pull cache files from remote (async implementation)."""
    _t = metrics.start()
    jobs = jobs if jobs is not None else config.get_remote_jobs()

    _check_remote_url(state_db, remote_name, remote)

    needed_hashes = get_needed_hashes(
        targets, state_dir, all_stages, project.get_project_root(), exclude_patterns
    )

    if not needed_hashes:
        metrics.end("sync.pull_async", _t)
        return TransferSummary(transferred=0, skipped=0, failed=0, errors=[])

    local_hashes = get_local_cache_hashes(cache_dir)
    missing_locally = needed_hashes - local_hashes

    if not missing_locally:
        metrics.end("sync.pull_async", _t)
        return TransferSummary(transferred=0, skipped=len(needed_hashes), failed=0, errors=[])

    files_dir = cache_dir / "files"
    items = list[tuple[str, pathlib.Path]]()
    for hash_ in missing_locally:
        cache_path = cache.get_cache_path(files_dir, hash_)
        items.append((hash_, cache_path))

    download_callback = callback
    if callback is not None:
        name_by_hash = build_hash_path_index(state_dir, all_stages, project.get_project_root())

        def _translate(completed: float, total: int, ident: str) -> None:
            callback(completed, total, name_by_hash.get(ident, ident[:8]))

        download_callback = _translate

    results = await remote.download_batch(
        items,
        concurrency=jobs,
        callback=download_callback,
        readonly=True,
        byte_callback=byte_callback,
    )

    transferred = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    state_db.remote_hashes_add(remote_name, [r["hash"] for r in transferred])
    errors = [r["error"] for r in failed if "error" in r]

    metrics.end("sync.pull_async", _t)
    return TransferSummary(
        transferred=len(transferred),
        skipped=len(needed_hashes) - len(missing_locally),
        failed=len(failed),
        errors=errors,
    )


def pull(
    cache_dir: pathlib.Path,
    state_dir: pathlib.Path,
    remote: remote_mod.S3Remote,
    state_db: state_mod.StateDB,
    remote_name: str,
    targets: list[str] | None = None,
    jobs: int | None = None,
    callback: Callable[[float, int, str], None] | None = None,
    all_stages: dict[str, RegistryStageInfo] | None = None,
    exclude_patterns: list[str] | None = None,
    byte_callback: Callable[[int], None] | None = None,
) -> TransferSummary:
    """Pull cache files from remote storage."""
    return asyncio.run(
        _pull_async(
            cache_dir,
            state_dir,
            remote,
            state_db,
            remote_name,
            targets,
            jobs,
            callback,
            all_stages,
            exclude_patterns,
            byte_callback,
        )
    )


def create_remote_from_name(name: str | None = None) -> tuple[remote_mod.S3Remote, str]:
    """Create S3Remote from configured remote name. Returns (remote, name)."""
    url = remote_config.get_remote_url(name)
    resolved_name = name
    if resolved_name is None:
        resolved_name = remote_config.get_default_remote()
        if resolved_name is None:
            remotes = remote_config.list_remotes()
            if len(remotes) == 1:
                resolved_name = next(iter(remotes.keys()))
            else:
                raise exceptions.RemoteNotFoundError("Could not determine remote name")

    return remote_mod.S3Remote(url), resolved_name
