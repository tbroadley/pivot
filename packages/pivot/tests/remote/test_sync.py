from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pivot import loaders
from pivot import outputs as outputs_mod
from pivot.registry import RegistryStageInfo
from pivot.remote import storage as remote_storage
from pivot.remote import sync
from pivot.storage import cache as cache_mod
from pivot.storage import lock, track
from pivot.storage import state as state_mod
from pivot.types import DirHash, DirManifestEntry, FileHash, LockData, TransferResult

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    from pytest_mock import MockerFixture

# =============================================================================
# Unit tests for _extract_file_hashes_from_hash_info
# =============================================================================


def test_extract_file_hashes_from_file_hash() -> None:
    """FileHash returns its hash as the only element."""
    fh = FileHash(hash="abcdef1234567890")
    result = sync._extract_file_hashes_from_hash_info(fh)
    assert result == {"abcdef1234567890"}


def test_extract_file_hashes_from_dir_hash_excludes_tree_hash() -> None:
    """DirHash returns only manifest file hashes, not the tree hash."""
    dh = DirHash(
        hash="aaaaaaaaaaaaaaaa",  # tree hash — must be excluded
        manifest=[
            DirManifestEntry(relpath="a.csv", hash="1111111111111111", size=100, isexec=False),
            DirManifestEntry(relpath="b.csv", hash="2222222222222222", size=200, isexec=False),
        ],
    )
    result = sync._extract_file_hashes_from_hash_info(dh)
    assert result == {"1111111111111111", "2222222222222222"}
    assert "aaaaaaaaaaaaaaaa" not in result


def test_extract_file_hashes_from_dir_hash_empty_manifest() -> None:
    """DirHash with empty manifest returns empty set (tree hash excluded)."""
    dh = DirHash(hash="aaaaaaaaaaaaaaaa", manifest=[])
    result = sync._extract_file_hashes_from_hash_info(dh)
    assert result == set()


# =============================================================================
# Integration tests for get_stage_output_hashes / get_stage_dep_hashes
# =============================================================================


def _write_lock_with_dir_output(
    stages_dir: pathlib.Path,
    stage_name: str,
    tree_hash: str,
    file_hashes: list[str],
) -> None:
    """Helper: write a lock file with a directory output containing a tree hash."""
    manifest = [
        DirManifestEntry(relpath=f"file{i}.csv", hash=h, size=100, isexec=False)
        for i, h in enumerate(file_hashes)
    ]
    dir_hash = DirHash(hash=tree_hash, manifest=manifest)
    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={"output_dir": dir_hash},
    )
    stage_lock = lock.StageLock(stage_name, stages_dir)
    stage_lock.write(lock_data)


def test_get_stage_output_hashes_excludes_tree_hash(set_project_root: pathlib.Path) -> None:
    """get_stage_output_hashes returns file hashes only, not tree hashes."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    tree_hash = "aaaaaaaaaaaaaaaa"
    file_hashes = ["1111111111111111", "2222222222222222"]
    _write_lock_with_dir_output(stages_dir, "my_stage", tree_hash, file_hashes)

    result = sync.get_stage_output_hashes(state_dir, ["my_stage"])

    assert "1111111111111111" in result
    assert "2222222222222222" in result
    assert tree_hash not in result


def test_get_stage_dep_hashes_excludes_tree_hash(set_project_root: pathlib.Path) -> None:
    """get_stage_dep_hashes returns file hashes only, not tree hashes."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    dep_manifest = [
        DirManifestEntry(relpath="dep.csv", hash="3333333333333333", size=50, isexec=False),
    ]
    dep_hash = DirHash(hash="bbbbbbbbbbbbbbbb", manifest=dep_manifest)
    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={str(set_project_root / "input_dir"): dep_hash},
        output_hashes={},
    )
    stage_lock = lock.StageLock("my_stage", stages_dir)
    stage_lock.write(lock_data)

    result = sync.get_stage_dep_hashes(state_dir, ["my_stage"])

    assert "3333333333333333" in result
    assert "bbbbbbbbbbbbbbbb" not in result


# =============================================================================
# get_target_hashes edge cases
# =============================================================================


def _helper_out_cache_false(path: str) -> outputs_mod.Out[pathlib.Path]:
    """Factory for Out(cache=False) — used in parametrized tests."""
    return outputs_mod.Out(path=path, loader=loaders.PathOnly(), cache=False)


def _helper_metric(path: str) -> outputs_mod.Metric:
    """Factory for Metric — used in parametrized tests."""
    return outputs_mod.Metric(path=path)


def test_get_target_hashes_invalid_stage_name_falls_through(
    set_project_root: pathlib.Path,
) -> None:
    """Target with invalid stage name chars (e.g. spaces) falls through to file path resolution."""
    state_dir = set_project_root / ".pivot"
    (state_dir / "stages").mkdir(parents=True, exist_ok=True)

    # "my data.csv" has a space, which is invalid for stage names.
    # Previously this would raise ValueError from StageLock.__init__.
    # After the fix it should fall through and end up in `unresolved`.
    result = sync.get_target_hashes(["my data.csv"], state_dir)
    assert result == set()


@pytest.mark.parametrize(
    "make_noncached",
    [
        pytest.param(_helper_out_cache_false, id="out-cache-false"),
        pytest.param(_helper_metric, id="metric"),
    ],
)
def test_get_target_hashes_excludes_noncached_outputs(
    set_project_root: pathlib.Path,
    make_noncached: Callable[[str], outputs_mod.BaseOut],
) -> None:
    """get_target_hashes excludes cache=False outputs from returned hashes."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    cached_out = outputs_mod.Out(
        path=str(set_project_root / "output.csv"),
        loader=loaders.PathOnly(),
        cache=True,
    )
    noncached = make_noncached(str(set_project_root / "metrics.json"))

    expanded_cached = outputs_mod.require_expanded(cached_out)
    expanded_noncached = outputs_mod.require_expanded(noncached)

    all_stages = {
        "my_stage": RegistryStageInfo(  # pyright: ignore[reportCallIssue] - partial for test
            state_dir=None,
            outs=[expanded_cached, expanded_noncached],
        )
    }

    cached_hash = FileHash(hash="1111111111111111")
    noncached_hash = FileHash(hash="2222222222222222")
    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={
            str(set_project_root / "output.csv"): cached_hash,
            str(set_project_root / "metrics.json"): noncached_hash,
        },
    )
    lock.StageLock("my_stage", stages_dir).write(lock_data)

    result = sync.get_target_hashes(["my_stage"], state_dir, all_stages=all_stages)

    assert "1111111111111111" in result, "Cached output hash should be included"
    assert "2222222222222222" not in result, "Non-cached output hash should be excluded"


def test_get_target_hashes_file_target_excludes_noncached(
    set_project_root: pathlib.Path,
) -> None:
    """File-path target for a cache=False output returns no hashes."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    # Stage with one cached and one non-cached output
    cached_out = outputs_mod.Out(
        path=str(set_project_root / "output.csv"),
        loader=loaders.PathOnly(),
        cache=True,
    )
    metric_out = outputs_mod.Metric(path=str(set_project_root / "metrics.json"))

    expanded_cached = outputs_mod.require_expanded(cached_out)
    expanded_metric = outputs_mod.require_expanded(metric_out)

    all_stages = {
        "my_stage": RegistryStageInfo(  # pyright: ignore[reportCallIssue] - partial for test
            state_dir=None,
            outs=[expanded_cached, expanded_metric],
        )
    }

    cached_hash = FileHash(hash="1111111111111111")
    metric_hash = FileHash(hash="2222222222222222")
    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={
            str(set_project_root / "output.csv"): cached_hash,
            str(set_project_root / "metrics.json"): metric_hash,
        },
    )
    lock.StageLock("my_stage", stages_dir).write(lock_data)

    # Target the non-cached file directly by path (not stage name)
    # This exercises _get_file_hash_from_stages (sync.py:107-124)
    result = sync.get_target_hashes(
        [str(set_project_root / "metrics.json")], state_dir, all_stages=all_stages
    )
    assert "2222222222222222" not in result, "Non-cached file target should return no hashes"

    # Target the cached file directly — should return its hash
    result_cached = sync.get_target_hashes(
        [str(set_project_root / "output.csv")], state_dir, all_stages=all_stages
    )
    assert "1111111111111111" in result_cached, "Cached file target should return its hash"


# =============================================================================
# get_referenced_hashes / get_needed_hashes
# =============================================================================


def _helper_stage_with_outs(*outs: outputs_mod.BaseOut) -> RegistryStageInfo:
    return RegistryStageInfo(  # pyright: ignore[reportCallIssue] - partial for test
        state_dir=None,
        outs=[outputs_mod.require_expanded(out) for out in outs],
    )


def test_get_referenced_hashes_excludes_raw_input_deps_and_noncached(
    set_project_root: pathlib.Path,
) -> None:
    """Referenced hashes include cached outputs, but not cache=False outputs nor
    raw-input deps (whose contents are never cached/pushed)."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    cached_out = outputs_mod.Out(
        path=str(set_project_root / "output.csv"), loader=loaders.PathOnly(), cache=True
    )
    metric_out = outputs_mod.Metric(path=str(set_project_root / "metrics.json"))
    all_stages = {"my_stage": _helper_stage_with_outs(cached_out, metric_out)}

    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={str(set_project_root / "in.csv"): FileHash(hash="3333333333333333")},
        output_hashes={
            str(set_project_root / "output.csv"): FileHash(hash="1111111111111111"),
            str(set_project_root / "metrics.json"): FileHash(hash="2222222222222222"),
        },
    )
    lock.StageLock("my_stage", stages_dir).write(lock_data)

    result = sync.get_referenced_hashes(state_dir, all_stages, set_project_root)

    assert result == {"1111111111111111"}


def test_get_referenced_hashes_includes_tracked_files(
    set_project_root: pathlib.Path,
) -> None:
    """Referenced hashes include .pvt-tracked file hashes."""
    state_dir = set_project_root / ".pivot"
    track.write_pvt_file(
        set_project_root / "data.csv.pvt",
        track.PvtData(path="data.csv", hash="abababababababab", size=10),
    )

    result = sync.get_referenced_hashes(state_dir, None, set_project_root)

    assert result == {"abababababababab"}


def test_get_referenced_hashes_ignores_unregistered_stage_lock(
    set_project_root: pathlib.Path,
) -> None:
    """A lock file for a stage absent from the registry is not pulled (stale state)."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={str(set_project_root / "old.csv"): FileHash(hash="deaddeaddeaddead")},
    )
    lock.StageLock("removed_stage", stages_dir).write(lock_data)

    result = sync.get_referenced_hashes(state_dir, {}, set_project_root)

    assert result == set()


def test_get_needed_hashes_delegates_to_target_hashes(
    set_project_root: pathlib.Path,
) -> None:
    """With targets, get_needed_hashes resolves only those targets (not all references)."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "output.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"my_stage": _helper_stage_with_outs(out)}
    lock.StageLock("my_stage", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={},
            output_hashes={str(set_project_root / "output.csv"): FileHash(hash="1111111111111111")},
        )
    )
    track.write_pvt_file(
        set_project_root / "data.csv.pvt",
        track.PvtData(path="data.csv", hash="abababababababab", size=10),
    )

    targeted = sync.get_needed_hashes(["my_stage"], state_dir, all_stages, set_project_root)
    assert targeted == {"1111111111111111"}, "Targets should not include unrelated tracked files"

    all_refs = sync.get_needed_hashes(None, state_dir, all_stages, set_project_root)
    assert all_refs == {"1111111111111111", "abababababababab"}


# =============================================================================
# Task 3: Push skips directory cache paths
# =============================================================================


async def test_push_skips_directory_cache_paths(
    tmp_path: pathlib.Path, mocker: MockerFixture
) -> None:
    """Push should never enqueue directory paths for upload."""
    cache_dir = tmp_path / "cache"
    state_dir = tmp_path / ".pivot"
    state_dir.mkdir(parents=True)
    files_dir = cache_dir / "files"

    # Create a file cache entry
    file_hash = "1111111111111111"
    file_cache = files_dir / file_hash[:2] / file_hash[2:]
    file_cache.parent.mkdir(parents=True)
    file_cache.write_text("file content")

    # Create a directory cache entry (simulating SYMLINK mode tree hash)
    dir_hash = "aaaaaaaaaaaaaaaa"
    dir_cache = files_dir / dir_hash[:2] / dir_hash[2:]
    dir_cache.mkdir(parents=True)
    (dir_cache / "some_file.csv").write_text("data")

    # Verify preconditions: both entries exist, one is a file, one is a directory
    file_path = cache_mod.get_cache_path(files_dir, file_hash)
    dir_path = cache_mod.get_cache_path(files_dir, dir_hash)
    assert file_path.is_file(), "File cache entry should be a file"
    assert dir_path.is_dir(), "Dir cache entry should be a directory"

    mock_remote = mocker.Mock(spec=remote_storage.S3Remote)
    mock_state = mocker.Mock(spec=state_mod.StateDB)
    # Both hashes are in local cache; none known on remote
    mock_state.remote_hashes_intersection.return_value = set()
    mock_remote.bulk_exists = mocker.AsyncMock(return_value={file_hash: False, dir_hash: False})
    mock_remote.upload_batch = mocker.AsyncMock(
        return_value=[TransferResult(hash=file_hash, success=True)]
    )

    # Mock get_local_cache_hashes to return both file and directory hashes
    # (normally it filters out directories, but we want to test the filtering in _push_async)
    mocker.patch.object(sync, "get_local_cache_hashes", return_value={file_hash, dir_hash})

    result = await sync._push_async(cache_dir, state_dir, mock_remote, mock_state, "origin")

    # upload_batch should only receive the file entry, not the directory
    mock_remote.upload_batch.assert_called_once()
    uploaded_items = mock_remote.upload_batch.call_args[0][0]
    uploaded_hashes = {h for _, h in uploaded_items}
    assert file_hash in uploaded_hashes, "File hash should be uploaded"
    assert dir_hash not in uploaded_hashes, "Directory hash should be skipped"
    assert result["transferred"] == 1
    # Verify directory was counted in skipped total (skipped_non_file fix from Task 3)
    assert result["skipped"] == 1, "Directory cache path should be counted in skipped total"


# =============================================================================
# get_target_hashes: file path targets without pipeline
# =============================================================================


def test_get_target_hashes_file_path_with_lock_files_no_pipeline(
    set_project_root: pathlib.Path,
) -> None:
    """File path target resolves via lock file even without a pipeline."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    abs_output = str(set_project_root / "data" / "output.csv")
    output_hash = FileHash(hash="aabbccdd11223344")
    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={abs_output: output_hash},
    )
    lock.StageLock("train", stages_dir).write(lock_data)

    result = sync.get_target_hashes([abs_output], state_dir, all_stages=None)

    assert result == {"aabbccdd11223344"}, "Should resolve hash from lock file without pipeline"


def test_get_target_hashes_pvt_file_target_without_pipeline(
    set_project_root: pathlib.Path,
) -> None:
    """File path target resolves via .pvt tracking file without a pipeline."""
    state_dir = set_project_root / ".pivot"
    (state_dir / "stages").mkdir(parents=True, exist_ok=True)

    pvt_path = set_project_root / "data" / "input.csv.pvt"
    pvt_path.parent.mkdir(parents=True, exist_ok=True)
    track.write_pvt_file(
        pvt_path, track.PvtData(path="input.csv", hash="eeff00112233aabb", size=100)
    )

    result = sync.get_target_hashes(
        [str(set_project_root / "data" / "input.csv")], state_dir, all_stages=None
    )

    assert result == {"eeff00112233aabb"}, "Should resolve hash from .pvt file without pipeline"


def test_get_target_hashes_pvt_suffix_stripped_before_lookup(
    set_project_root: pathlib.Path,
) -> None:
    """Target with .pvt suffix is stripped before lookup, avoiding .pvt.pvt."""
    state_dir = set_project_root / ".pivot"
    (state_dir / "stages").mkdir(parents=True, exist_ok=True)

    pvt_path = set_project_root / "data" / "result.csv.pvt"
    pvt_path.parent.mkdir(parents=True, exist_ok=True)
    track.write_pvt_file(
        pvt_path, track.PvtData(path="result.csv", hash="1122334455667788", size=100)
    )

    result = sync.get_target_hashes(
        [str(set_project_root / "data" / "result.csv.pvt")], state_dir, all_stages=None
    )

    assert result == {"1122334455667788"}, ".pvt suffix should be stripped before hash lookup"


def test_get_target_hashes_unresolved_file_target_returns_empty(
    set_project_root: pathlib.Path,
) -> None:
    """File target that matches nothing returns empty set without crashing."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    lock_data = LockData(
        code_manifest={},
        params={},
        dep_hashes={},
        output_hashes={str(set_project_root / "other" / "file.csv"): FileHash(hash="aaaa")},
    )
    lock.StageLock("train", stages_dir).write(lock_data)

    result = sync.get_target_hashes(
        [str(set_project_root / "nonexistent.csv")], state_dir, all_stages=None
    )

    assert result == set(), "Unresolved target should return empty set, not crash"


# =============================================================================
# Exclusion via --exclude patterns
# =============================================================================


def test_exclude_drops_dependency_hash(set_project_root: pathlib.Path) -> None:
    """Excluding a dep path drops its hash (the combine_runs deps leak)."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "out.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"combine_runs": _helper_stage_with_outs(out)}
    lock.StageLock("combine_runs", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={
                str(set_project_root / "data/raw/sensitive/scans"): FileHash(hash="5ec5e7"),
            },
            output_hashes={str(set_project_root / "out.csv"): FileHash(hash="0117")},
        )
    )

    result = sync.get_needed_hashes(
        None, state_dir, all_stages, set_project_root, exclude_patterns=["data/raw/sensitive"]
    )

    assert result == {"0117"}, "Excluded dep hash must not be accumulated; output stays"


def test_exclude_drops_output_and_tracked_pvt(set_project_root: pathlib.Path) -> None:
    """Excluding a path drops both stage outputs and standalone .pvt files under it."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    keep_out = outputs_mod.Out(
        path=str(set_project_root / "public/out.csv"), loader=loaders.PathOnly(), cache=True
    )
    drop_out = outputs_mod.Out(
        path=str(set_project_root / "data/raw/sensitive/out.csv"),
        loader=loaders.PathOnly(),
        cache=True,
    )
    all_stages = {"s": _helper_stage_with_outs(keep_out, drop_out)}
    lock.StageLock("s", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={},
            output_hashes={
                str(set_project_root / "public/out.csv"): FileHash(hash="keep01"),
                str(set_project_root / "data/raw/sensitive/out.csv"): FileHash(hash="drop01"),
            },
        )
    )
    track.write_pvt_file(
        set_project_root / "data/raw/sensitive/transcript_annotation.pvt",
        track.PvtData(path="transcript_annotation", hash="drop02", size=10),
    )

    result = sync.get_needed_hashes(
        None, state_dir, all_stages, set_project_root, exclude_patterns=["data/raw/sensitive"]
    )

    assert result == {"keep01"}, "Excluded output and .pvt hashes must be dropped"


def test_exclude_drops_directory_artifact_manifest(set_project_root: pathlib.Path) -> None:
    """Excluding a directory artifact drops every file hash in its manifest."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    dir_hash = DirHash(
        hash="treehash",
        manifest=[
            DirManifestEntry(relpath="a", hash="dir01", size=1, isexec=False),
            DirManifestEntry(relpath="b", hash="dir02", size=2, isexec=False),
        ],
    )
    all_stages = {"s": _helper_stage_with_outs()}
    lock.StageLock("s", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "data/raw/sensitive/scans"): dir_hash},
            output_hashes={},
        )
    )

    result = sync.get_needed_hashes(
        None, state_dir, all_stages, set_project_root, exclude_patterns=["data/raw/sensitive"]
    )

    assert result == set(), "All manifest hashes of an excluded directory must be dropped"


def test_exclude_retains_hash_shared_with_nonexcluded_path(
    set_project_root: pathlib.Path,
) -> None:
    """A hash referenced by both an excluded and a non-excluded path is retained."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "public/copy.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"s": _helper_stage_with_outs(out)}
    lock.StageLock("s", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={
                str(set_project_root / "data/raw/sensitive/orig.csv"): FileHash(hash="dup")
            },
            output_hashes={str(set_project_root / "public/copy.csv"): FileHash(hash="dup")},
        )
    )

    result = sync.get_needed_hashes(
        None, state_dir, all_stages, set_project_root, exclude_patterns=["data/raw/sensitive"]
    )

    assert result == {"dup"}, "Shared hash stays because the non-excluded path still references it"


def test_exclude_prefix_does_not_match_sibling(set_project_root: pathlib.Path) -> None:
    """`data/raw/sensitive` excludes nested paths but not a sibling like `sensitive2`."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    nested_out = outputs_mod.Out(
        path=str(set_project_root / "data/raw/sensitive/scans"),
        loader=loaders.PathOnly(),
        cache=True,
    )
    sibling_out = outputs_mod.Out(
        path=str(set_project_root / "data/raw/sensitive2/x.csv"),
        loader=loaders.PathOnly(),
        cache=True,
    )
    all_stages = {"s": _helper_stage_with_outs(nested_out, sibling_out)}
    lock.StageLock("s", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={},
            output_hashes={
                str(set_project_root / "data/raw/sensitive/scans"): FileHash(hash="nested"),
                str(set_project_root / "data/raw/sensitive2/x.csv"): FileHash(hash="sibling"),
            },
        )
    )

    result = sync.get_needed_hashes(
        None, state_dir, all_stages, set_project_root, exclude_patterns=["data/raw/sensitive"]
    )

    assert result == {"sibling"}, "Sibling dir sharing a name prefix must not be excluded"


def test_exclude_applies_with_explicit_targets(set_project_root: pathlib.Path) -> None:
    """Exclusion also filters deps when explicit stage targets are given."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "out.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"combine_runs": _helper_stage_with_outs(out)}
    lock.StageLock("combine_runs", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "data/raw/sensitive/scans"): FileHash(hash="dep01")},
            output_hashes={str(set_project_root / "out.csv"): FileHash(hash="out01")},
        )
    )

    result = sync.get_needed_hashes(
        ["combine_runs"],
        state_dir,
        all_stages,
        set_project_root,
        exclude_patterns=["data/raw/sensitive"],
    )

    assert result == {"out01"}, "Explicit-target pulls must also drop excluded deps"


# =============================================================================
# include_deps only fetches cacheable artifacts (issue #460)
# =============================================================================


def test_pull_excludes_raw_input_dep(set_project_root: pathlib.Path) -> None:
    """A raw-input dep (not a stage output, not .pvt) is never fetched on pull."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "out.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"my_stage": _helper_stage_with_outs(out)}
    lock.StageLock("my_stage", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "spec.yaml"): FileHash(hash="rawinput")},
            output_hashes={str(set_project_root / "out.csv"): FileHash(hash="out01")},
        )
    )

    result = sync.get_needed_hashes(["my_stage"], state_dir, all_stages, set_project_root)

    assert result == {"out01"}, "Raw-input dep hash must not be fetched"


def test_pull_includes_upstream_cached_output_dep(set_project_root: pathlib.Path) -> None:
    """A dep produced by an upstream cached stage is fetched on a targeted pull."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    upstream_out = outputs_mod.Out(
        path=str(set_project_root / "upstream.csv"), loader=loaders.PathOnly(), cache=True
    )
    downstream_out = outputs_mod.Out(
        path=str(set_project_root / "downstream.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {
        "upstream": _helper_stage_with_outs(upstream_out),
        "downstream": _helper_stage_with_outs(downstream_out),
    }
    lock.StageLock("upstream", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={},
            output_hashes={str(set_project_root / "upstream.csv"): FileHash(hash="upstream01")},
        )
    )
    lock.StageLock("downstream", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "upstream.csv"): FileHash(hash="upstream01")},
            output_hashes={str(set_project_root / "downstream.csv"): FileHash(hash="downstream01")},
        )
    )

    result = sync.get_needed_hashes(["downstream"], state_dir, all_stages, set_project_root)

    assert result == {"downstream01", "upstream01"}, "Upstream cached output dep must be fetched"


def test_pull_includes_pvt_tracked_dep(set_project_root: pathlib.Path) -> None:
    """A dep that is a .pvt-tracked artifact is fetched on a targeted pull."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    out = outputs_mod.Out(
        path=str(set_project_root / "out.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {"my_stage": _helper_stage_with_outs(out)}
    lock.StageLock("my_stage", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "data.csv"): FileHash(hash="tracked01")},
            output_hashes={str(set_project_root / "out.csv"): FileHash(hash="out01")},
        )
    )
    track.write_pvt_file(
        set_project_root / "data.csv.pvt",
        track.PvtData(path="data.csv", hash="tracked01", size=10),
    )

    result = sync.get_needed_hashes(["my_stage"], state_dir, all_stages, set_project_root)

    assert result == {"out01", "tracked01"}, ".pvt-tracked dep must be fetched"


def test_pull_excludes_dep_from_noncached_upstream_output(set_project_root: pathlib.Path) -> None:
    """A dep produced by an upstream output with cache=False is not fetched."""
    state_dir = set_project_root / ".pivot"
    stages_dir = lock.get_stages_dir(state_dir)
    stages_dir.mkdir(parents=True, exist_ok=True)

    upstream_out = outputs_mod.Out(
        path=str(set_project_root / "upstream.csv"), loader=loaders.PathOnly(), cache=False
    )
    downstream_out = outputs_mod.Out(
        path=str(set_project_root / "downstream.csv"), loader=loaders.PathOnly(), cache=True
    )
    all_stages = {
        "upstream": _helper_stage_with_outs(upstream_out),
        "downstream": _helper_stage_with_outs(downstream_out),
    }
    lock.StageLock("upstream", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={},
            output_hashes={str(set_project_root / "upstream.csv"): FileHash(hash="upstream01")},
        )
    )
    lock.StageLock("downstream", stages_dir).write(
        LockData(
            code_manifest={},
            params={},
            dep_hashes={str(set_project_root / "upstream.csv"): FileHash(hash="upstream01")},
            output_hashes={str(set_project_root / "downstream.csv"): FileHash(hash="downstream01")},
        )
    )

    result = sync.get_needed_hashes(["downstream"], state_dir, all_stages, set_project_root)

    assert result == {"downstream01"}, "Dep from a cache=False upstream output must not be fetched"
