"""Cache garbage collection: find blobs no live reference points to.

The referenced ("live") set is every cached stage output and dependency recorded
in lock files, plus every ``.pvt``-tracked artifact. ``pivot gc`` removes local
cache blobs outside that set.

Collection is registry-free: it reads lock files and ``.pvt`` files directly
(from a worktree's working tree or from a git revision) rather than loading each
worktree's pipeline, which may be on a different code version. Extra hashes in
the referenced set are always safe -- they simply protect more blobs from
deletion.
"""

from __future__ import annotations

import enum
import logging
import os
import pathlib
from typing import TYPE_CHECKING, cast

import yaml

from pivot import git, project, yaml_config
from pivot.storage import lock, track

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

logger = logging.getLogger(__name__)


class GcScope(enum.Enum):
    """Which references keep a blob alive."""

    WORKSPACE = "workspace"  # current checkout only
    ALL = "all"  # every linked worktree + every local branch


def _hashes_from_manifest(manifest: object) -> set[str]:
    """Per-file blob hashes from a directory manifest, tolerating malformed shapes.

    Lock/.pvt structure validators (``is_lock_data``/``is_pvt_data``) check only
    top-level keys, so a manifest may be null or hold entries lacking ``hash``.
    Such entries are skipped rather than crashing the whole gc run -- lock files
    from mixed code versions must be tolerated.
    """
    if not isinstance(manifest, list):
        logger.debug("Skipping non-list manifest in lock/.pvt data")
        return set[str]()
    hashes = set[str]()
    for entry in cast("list[object]", manifest):
        if not isinstance(entry, dict):
            logger.debug("Skipping non-dict manifest entry")
            continue
        entry_dict = cast("dict[str, object]", entry)
        if "hash" not in entry_dict:
            logger.debug("Skipping manifest entry without hash")
            continue
        blob_hash = entry_dict["hash"]
        if isinstance(blob_hash, str):
            hashes.add(blob_hash)
        else:
            logger.debug("Skipping manifest entry with non-str hash")
    return hashes


def _hashes_from_ref(ref: object) -> set[str]:
    """Blob hashes for one lock deps/outs entry or a whole ``.pvt`` document.

    Directory refs contribute their per-file manifest hashes; the top-level tree
    hash is not a cached blob. File refs contribute the entry hash itself.
    Malformed refs contribute nothing instead of raising.
    """
    if not isinstance(ref, dict):
        return set[str]()
    ref_dict = cast("dict[str, object]", ref)
    if "manifest" in ref_dict:
        return _hashes_from_manifest(ref_dict["manifest"])
    if "hash" in ref_dict:
        blob_hash = ref_dict["hash"]
        if isinstance(blob_hash, str):
            return {blob_hash}
    return set[str]()


def _hashes_from_lock_bytes(raw: bytes | str) -> set[str]:
    """Blob hashes referenced by a single lock file's contents."""
    try:
        data = yaml.load(raw, Loader=yaml_config.Loader)
    except yaml.YAMLError:
        return set[str]()
    if not lock.is_lock_data(data):
        return set[str]()
    hashes = set[str]()
    for entries in (data["deps"], data["outs"]):
        for entry in entries:
            hashes |= _hashes_from_ref(entry)
    return hashes


def _hashes_from_pvt_bytes(raw: bytes | str) -> set[str]:
    """Blob hashes referenced by a single ``.pvt`` file's contents."""
    try:
        data = yaml.load(raw, Loader=yaml_config.Loader)
    except yaml.YAMLError:
        return set[str]()
    if not track.is_pvt_data(data):
        return set[str]()
    return _hashes_from_ref(data)


# Directories never worth walking for .pvt files; skipping them (especially a
# main worktree's real .git object store) is a large speedup.
_PRUNE_DIRS = frozenset({".git", ".venv", "venv", "node_modules", "__pycache__", ".pivot"})


def _iter_pvt_files(root: pathlib.Path) -> Iterator[pathlib.Path]:
    """Yield ``*.pvt`` paths under root, pruning version-control/env directories."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _PRUNE_DIRS]
        for fname in filenames:
            if fname.endswith(".pvt"):
                yield pathlib.Path(dirpath) / fname


def referenced_hashes_in_tree(root: pathlib.Path) -> set[str]:
    """Blob hashes referenced by the on-disk working tree at ``root``.

    Scans ``root/.pivot/stages/**/*.lock`` and every ``root/**/*.pvt`` file.
    Includes uncommitted lock/tracking changes -- the case most likely to hold
    blobs not yet on the remote.
    """
    hashes = set[str]()
    stages_dir = root / lock.STAGES_REL_PATH
    if stages_dir.is_dir():
        for lock_path in stages_dir.rglob("*.lock"):
            try:
                hashes |= _hashes_from_lock_bytes(lock_path.read_bytes())
            except OSError as exc:
                logger.debug(f"Could not read lock file {lock_path}: {exc}")
    for pvt_path in _iter_pvt_files(root):
        try:
            hashes |= _hashes_from_pvt_bytes(pvt_path.read_bytes())
        except OSError as exc:
            logger.debug(f"Could not read .pvt file {pvt_path}: {exc}")
    return hashes


def referenced_hashes_at_revisions(revs: Sequence[str]) -> set[str]:
    """Blob hashes referenced by committed lock/tracking files across revisions.

    Each unique blob is read once even when it appears in many revisions, so
    scanning every local branch is cheap despite near-identical lock files.
    """
    hashes = set[str]()
    for raw in git.read_matching_blobs_across_revisions(revs, lock.STAGES_REL_PATH, "*.lock"):
        hashes |= _hashes_from_lock_bytes(raw)
    for raw in git.read_matching_blobs_across_revisions(revs, "", "*.pvt"):
        hashes |= _hashes_from_pvt_bytes(raw)
    return hashes


def _find_git_dir(start: pathlib.Path) -> pathlib.Path | None:
    """Return the ``.git`` file/dir for the worktree containing ``start``."""
    for parent in [start, *start.parents]:
        candidate = parent / ".git"
        if candidate.exists():
            return candidate
    return None


def _common_git_dir(git_path: pathlib.Path) -> pathlib.Path | None:
    """Resolve the shared git directory from a worktree's ``.git`` file or dir."""
    if git_path.is_dir():
        return git_path
    try:
        content = git_path.read_text().strip()
    except OSError:
        return None
    # Linked worktree: ".git" is a file "gitdir: <common>/.git/worktrees/<name>".
    # With worktree.useRelativePaths (git >= 2.48) the target is relative to the
    # ".git" file's directory, so anchor it there before resolving.
    _, _, target = content.partition("gitdir:")
    target = target.strip()
    if not target:
        return None
    gitdir = pathlib.Path(target)
    if not gitdir.is_absolute():
        gitdir = git_path.parent / gitdir
    gitdir = gitdir.resolve()
    return gitdir.parent.parent if gitdir.name else None


def worktree_roots(start: pathlib.Path | None = None) -> list[pathlib.Path]:
    """Working-tree roots for the main repo and every linked worktree.

    Returns ``[start]`` (or the project root) when not inside a git repo. Stale
    worktree entries pointing at deleted directories are skipped.
    """
    start = start or project.get_project_root()
    git_path = _find_git_dir(start)
    if git_path is None:
        return [start]

    common = _common_git_dir(git_path)
    if common is None:
        return [start]

    roots = [common.parent]  # main worktree
    worktrees_dir = common / "worktrees"
    if worktrees_dir.is_dir():
        for entry in sorted(worktrees_dir.iterdir()):
            gitdir_file = entry / "gitdir"
            if not gitdir_file.is_file():
                continue
            try:
                linked_target = gitdir_file.read_text().strip()
            except OSError:
                continue
            # With worktree.useRelativePaths the gitdir path is relative to the
            # "worktrees/<name>/" directory holding this file.
            linked_git = pathlib.Path(linked_target)
            if not linked_git.is_absolute():
                linked_git = gitdir_file.parent / linked_git
            wt_root = linked_git.resolve().parent
            if wt_root.is_dir():
                roots.append(wt_root)
            else:
                logger.debug(f"Skipping stale worktree (missing dir): {wt_root}")

    # Deduplicate while preserving order.
    seen = set[pathlib.Path]()
    unique = list[pathlib.Path]()
    for root in roots:
        resolved = root.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(root)
    return unique


def collect_referenced_hashes(scope: GcScope) -> set[str]:
    """Blob hashes kept alive under ``scope``.

    ``WORKSPACE``: the current checkout only. ``ALL``: every linked worktree's
    working tree plus every local branch's committed state.
    """
    if scope is GcScope.WORKSPACE:
        return referenced_hashes_in_tree(project.get_project_root())

    # worktree_roots() yields git worktree roots, but referenced_hashes_in_tree
    # expects the pivot project root. When .pivot lives in a subdirectory of the
    # git repo, apply that same prefix to each worktree root.
    prefix = git.get_project_prefix()
    hashes = set[str]()
    for root in worktree_roots():
        proj_root = root / prefix if prefix is not None else root
        hashes |= referenced_hashes_in_tree(proj_root)
    hashes |= referenced_hashes_at_revisions(git.list_local_branches())
    return hashes
