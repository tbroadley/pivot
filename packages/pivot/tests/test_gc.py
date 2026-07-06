from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import yaml

from pivot import gc, git, project
from pivot.storage import cache as cache_mod

if TYPE_CHECKING:
    import pathlib

    import pytest

    from tests.conftest import GitRepo


def _hash(char: str) -> str:
    return char * cache_mod.XXHASH64_HEX_LENGTH


def _file_entry(path: str, file_hash: str) -> dict[str, object]:
    return {"path": path, "hash": file_hash}


def _dir_entry(path: str, tree_hash: str, file_hashes: list[str]) -> dict[str, object]:
    manifest = [
        {"relpath": f"f{i}", "hash": h, "size": 1, "isexec": False}
        for i, h in enumerate(file_hashes)
    ]
    return {"path": path, "hash": tree_hash, "manifest": manifest}


def _lock_yaml(deps: list[dict[str, object]], outs: list[dict[str, object]]) -> str:
    return yaml.safe_dump(
        {"schema_version": 1, "code_manifest": {}, "params": {}, "deps": deps, "outs": outs}
    )


def _write_lock(stages_dir: pathlib.Path, name: str, content: str) -> None:
    stages_dir.mkdir(parents=True, exist_ok=True)
    (stages_dir / f"{name}.lock").write_text(content)


def _write_pvt(
    pvt_path: pathlib.Path, file_hash: str, manifest_hashes: list[str] | None = None
) -> None:
    data: dict[str, object] = {"path": pvt_path.stem, "hash": file_hash, "size": 1}
    if manifest_hashes is not None:
        data["manifest"] = [
            {"relpath": f"g{i}", "hash": h, "size": 1, "isexec": False}
            for i, h in enumerate(manifest_hashes)
        ]
    pvt_path.write_text(yaml.safe_dump(data))


def _make_blob(cache_dir: pathlib.Path, file_hash: str, content: bytes = b"x") -> pathlib.Path:
    files_dir = cache_dir / "files"
    blob = cache_mod.get_cache_path(files_dir, file_hash)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(content)
    blob.chmod(0o444)
    return blob


# =============================================================================
# Pure hash extraction
# =============================================================================


def test_hashes_from_lock_bytes_file_and_dir_entries() -> None:
    content = _lock_yaml(
        deps=[_file_entry("in.csv", _hash("a"))],
        outs=[_dir_entry("out/", _hash("b"), [_hash("c"), _hash("d")])],
    )
    assert gc._hashes_from_lock_bytes(content) == {_hash("a"), _hash("c"), _hash("d")}, (
        "dir tree hash must be excluded; manifest file hashes included"
    )


def test_hashes_from_lock_bytes_invalid_returns_empty() -> None:
    assert gc._hashes_from_lock_bytes("not: [valid: lock") == set()
    assert gc._hashes_from_lock_bytes(yaml.safe_dump({"deps": []})) == set()


def test_hashes_from_pvt_bytes_file_and_dir() -> None:
    file_pvt = yaml.safe_dump({"path": "d.csv", "hash": _hash("a"), "size": 1})
    assert gc._hashes_from_pvt_bytes(file_pvt) == {_hash("a")}

    dir_pvt = yaml.safe_dump(
        {
            "path": "d",
            "hash": _hash("b"),
            "size": 2,
            "manifest": [{"relpath": "x", "hash": _hash("c"), "size": 1, "isexec": False}],
        }
    )
    assert gc._hashes_from_pvt_bytes(dir_pvt) == {_hash("c")}, "dir uses manifest hashes"


def test_hashes_from_pvt_bytes_invalid_returns_empty() -> None:
    assert gc._hashes_from_pvt_bytes("{{bad") == set()
    assert gc._hashes_from_pvt_bytes(yaml.safe_dump({"path": "x"})) == set()


# =============================================================================
# Working-tree scan
# =============================================================================


def test_referenced_hashes_in_tree_collects_locks_and_pvt(tmp_path: pathlib.Path) -> None:
    stages = tmp_path / ".pivot" / "stages"
    _write_lock(
        stages,
        "stage_a",
        _lock_yaml([_file_entry("in", _hash("a"))], [_file_entry("out", _hash("b"))]),
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_pvt(data_dir / "raw.csv.pvt", _hash("c"))
    _write_pvt(data_dir / "tree.pvt", _hash("d"), manifest_hashes=[_hash("e")])

    assert gc.referenced_hashes_in_tree(tmp_path) == {
        _hash("a"),
        _hash("b"),
        _hash("c"),
        _hash("e"),
    }


def test_referenced_hashes_in_tree_no_stages_dir(tmp_path: pathlib.Path) -> None:
    assert gc.referenced_hashes_in_tree(tmp_path) == set()


# =============================================================================
# Worktree enumeration
# =============================================================================


def test_worktree_roots_non_git_returns_start(tmp_path: pathlib.Path) -> None:
    assert gc.worktree_roots(tmp_path) == [tmp_path]


def test_worktree_roots_includes_linked_worktrees(git_repo: GitRepo) -> None:
    repo_path, commit = git_repo
    (repo_path / "README").write_text("hi")
    commit("init")

    linked = repo_path.with_name(repo_path.name + "-wt")
    subprocess.run(
        ["git", "worktree", "add", "-b", "feature", str(linked)],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )

    roots = gc.worktree_roots(repo_path)
    resolved = {r.resolve() for r in roots}
    assert repo_path.resolve() in resolved
    assert linked.resolve() in resolved


def test_worktree_roots_resolves_from_inside_linked_worktree(git_repo: GitRepo) -> None:
    repo_path, commit = git_repo
    (repo_path / "README").write_text("hi")
    commit("init")

    linked = repo_path.with_name(repo_path.name + "-wt")
    subprocess.run(
        ["git", "worktree", "add", "-b", "feature", str(linked)],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )

    # Called from the linked worktree, whose ".git" is a file (not a directory).
    resolved = {r.resolve() for r in gc.worktree_roots(linked)}
    assert repo_path.resolve() in resolved
    assert linked.resolve() in resolved


def test_worktree_roots_skips_stale_worktree(git_repo: GitRepo) -> None:
    repo_path, commit = git_repo
    (repo_path / "README").write_text("hi")
    commit("init")

    linked = repo_path.with_name(repo_path.name + "-wt")
    subprocess.run(
        ["git", "worktree", "add", "-b", "feature", str(linked)],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )
    # Delete the worktree directory without pruning metadata -> stale entry.
    subprocess.run(["rm", "-rf", str(linked)], check=True)

    roots = gc.worktree_roots(repo_path)
    assert linked.resolve() not in {r.resolve() for r in roots}
    assert repo_path.resolve() in {r.resolve() for r in roots}


# =============================================================================
# Revision scan + branch listing
# =============================================================================


def test_referenced_hashes_at_revisions_reads_committed_files(
    git_repo: GitRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_path, commit = git_repo
    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    stages = repo_path / ".pivot" / "stages"
    _write_lock(stages, "s", _lock_yaml([], [_file_entry("out", _hash("a"))]))
    _write_pvt(repo_path / "raw.csv.pvt", _hash("b"))
    commit("add pipeline state")

    assert gc.referenced_hashes_at_revisions(["HEAD"]) == {_hash("a"), _hash("b")}


def test_referenced_hashes_at_revisions_dedups_shared_blobs(
    git_repo: GitRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_path, commit = git_repo
    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    stages = repo_path / ".pivot" / "stages"
    _write_lock(stages, "s", _lock_yaml([], [_file_entry("out", _hash("a"))]))
    commit("main state")
    subprocess.run(["git", "branch", "dup"], cwd=repo_path, check=True, capture_output=True)

    # Same committed lock file on both refs -> union still just the one hash.
    assert gc.referenced_hashes_at_revisions(["HEAD", "dup"]) == {_hash("a")}


def test_list_local_branches(git_repo: GitRepo, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_path, commit = git_repo
    monkeypatch.setattr(project, "_project_root_cache", repo_path)
    (repo_path / "f").write_text("x")
    commit("init")
    subprocess.run(["git", "branch", "other"], cwd=repo_path, check=True, capture_output=True)

    assert set(git.list_local_branches()) >= {"other"}


# =============================================================================
# Scope aggregation
# =============================================================================


def test_collect_referenced_hashes_workspace_vs_all(
    git_repo: GitRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_path, commit = git_repo
    monkeypatch.setattr(project, "_project_root_cache", repo_path)
    (repo_path / "f").write_text("x")
    commit("init")

    linked = repo_path.with_name(repo_path.name + "-wt")
    subprocess.run(
        ["git", "worktree", "add", "-b", "feature", str(linked)],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )
    _write_lock(
        repo_path / ".pivot" / "stages",
        "here",
        _lock_yaml([], [_file_entry("out", _hash("a"))]),
    )
    _write_lock(
        linked / ".pivot" / "stages",
        "there",
        _lock_yaml([], [_file_entry("out", _hash("b"))]),
    )

    workspace = gc.collect_referenced_hashes(gc.GcScope.WORKSPACE)
    assert _hash("a") in workspace
    assert _hash("b") not in workspace, "workspace scope excludes other worktrees"

    all_scope = gc.collect_referenced_hashes(gc.GcScope.ALL)
    assert {_hash("a"), _hash("b")} <= all_scope


# =============================================================================
# Cache blob helpers
# =============================================================================


def test_sum_blob_sizes(tmp_path: pathlib.Path) -> None:
    _make_blob(tmp_path, _hash("a"), b"12345")
    _make_blob(tmp_path, _hash("b"), b"678")
    assert cache_mod.sum_blob_sizes(tmp_path, {_hash("a"), _hash("b")}) == 8
    assert cache_mod.sum_blob_sizes(tmp_path, {_hash("a"), _hash("f")}) == 5, "missing counts as 0"


def test_remove_cache_blobs_frees_and_prunes_empty_prefix(tmp_path: pathlib.Path) -> None:
    blob = _make_blob(tmp_path, _hash("a"), b"1234")
    prefix_dir = blob.parent
    removed, freed = cache_mod.remove_cache_blobs(tmp_path, {_hash("a")})
    assert (removed, freed) == (1, 4)
    assert not blob.exists()
    assert not prefix_dir.exists(), "emptied prefix directory is pruned"


def test_remove_cache_blobs_keeps_nonempty_prefix(tmp_path: pathlib.Path) -> None:
    # Two blobs sharing a prefix: aaaa... and aabb...
    keep = "aa" + "b" * (cache_mod.XXHASH64_HEX_LENGTH - 2)
    _make_blob(tmp_path, _hash("a"))
    kept_blob = _make_blob(tmp_path, keep)
    removed, _freed = cache_mod.remove_cache_blobs(tmp_path, {_hash("a")})
    assert removed == 1
    assert kept_blob.exists(), "prefix retained because another blob remains"


def test_remove_cache_blobs_missing_hash_is_noop(tmp_path: pathlib.Path) -> None:
    assert cache_mod.remove_cache_blobs(tmp_path, {_hash("f")}) == (0, 0)
