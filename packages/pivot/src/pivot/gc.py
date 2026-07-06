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
from typing import TYPE_CHECKING

import yaml

from pivot import git, project
from pivot.storage import lock, track

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pivot.types import DepEntry, OutEntry

logger = logging.getLogger(__name__)

# gc parses hundreds of lock/.pvt files per worktree and revision, so use the
# libyaml-backed loader (~9x faster than the pure-Python SafeLoader) when
# available, falling back to the pure-Python loader otherwise.
_YamlLoader: type[yaml.SafeLoader] | type[yaml.CSafeLoader]
try:
    _YamlLoader = yaml.CSafeLoader
except AttributeError:  # libyaml not built into the PyYAML install
    _YamlLoader = yaml.SafeLoader


class GcScope(enum.Enum):
    """Which references keep a blob alive."""

    WORKSPACE = "workspace"  # current checkout only
    ALL = "all"  # every linked worktree + every local branch


def _hashes_from_entries(entries: list[DepEntry] | list[OutEntry]) -> set[str]:
    """Blob hashes for lock deps/outs entries.

    For directory entries the blobs are the per-file manifest hashes; the
    top-level tree hash is not a cached blob. For file entries the blob is the
    entry hash itself.
    """
    hashes = set[str]()
    for entry in entries:
        if "manifest" in entry:
            hashes.update(m["hash"] for m in entry["manifest"])
        else:
            hashes.add(entry["hash"])
    return hashes


def _hashes_from_lock_bytes(raw: bytes | str) -> set[str]:
    """Blob hashes referenced by a single lock file's contents."""
    try:
        data = yaml.load(raw, Loader=_YamlLoader)
    except yaml.YAMLError:
        return set[str]()
    if not lock.is_lock_data(data):
        return set[str]()
    return _hashes_from_entries(data["deps"]) | _hashes_from_entries(data["outs"])


def _hashes_from_pvt_bytes(raw: bytes | str) -> set[str]:
    """Blob hashes referenced by a single ``.pvt`` file's contents."""
    try:
        data = yaml.load(raw, Loader=_YamlLoader)
    except yaml.YAMLError:
        return set[str]()
    if not track.is_pvt_data(data):
        return set[str]()
    if "manifest" in data:
        return {entry["hash"] for entry in data["manifest"]}
    return {data["hash"]}


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
    _, _, target = content.partition("gitdir:")
    gitdir = pathlib.Path(target.strip())
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
                linked_git = pathlib.Path(gitdir_file.read_text().strip())
            except OSError:
                continue
            wt_root = linked_git.parent
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

    hashes = set[str]()
    for root in worktree_roots():
        hashes |= referenced_hashes_in_tree(root)
    hashes |= referenced_hashes_at_revisions(git.list_local_branches())
    return hashes
