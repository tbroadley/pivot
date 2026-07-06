from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, cast

import dulwich.objects
import dulwich.repo

from conftest import init_git_repo
from pivot import git, project

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import MonkeyPatch

    from conftest import GitRepo


# =============================================================================
# read_file_from_head Tests
# =============================================================================


def test_read_file_from_head_no_git_repo(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns None when not in a git repo."""
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.read_file_from_head("somefile.txt")

    assert result is None


def test_read_file_from_head_file_not_in_head(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Returns None when file doesn't exist in HEAD."""
    repo_path, commit = git_repo
    (repo_path / "other.txt").write_text("content")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.read_file_from_head("nonexistent.txt")

    assert result is None


def test_read_file_from_head_returns_content(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Returns file content from HEAD."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("committed content")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.read_file_from_head("file.txt")

    assert result == b"committed content"


def test_read_file_from_head_uncommitted_changes_not_visible(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    """Returns committed content, not uncommitted changes."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("original content")
    commit("initial")

    # Modify file but don't commit
    (repo_path / "file.txt").write_text("modified content")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.read_file_from_head("file.txt")

    assert result == b"original content"


def test_read_file_from_head_subdirectory(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Can read files in subdirectories."""
    repo_path, commit = git_repo
    (repo_path / "subdir").mkdir()
    (repo_path / "subdir" / "nested.txt").write_text("nested content")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.read_file_from_head("subdir/nested.txt")

    assert result == b"nested content"


def test_read_file_from_head_empty_repo(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns None for empty repo (no commits)."""
    init_git_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.read_file_from_head("file.txt")

    assert result is None


# =============================================================================
# read_files_from_head Tests
# =============================================================================


def test_read_files_from_head_empty_list(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns empty dict for empty path list."""
    init_git_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.read_files_from_head([])

    assert result == {}


def test_read_files_from_head_multiple_files(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Returns content for multiple files."""
    repo_path, commit = git_repo
    (repo_path / "file1.txt").write_text("content1")
    (repo_path / "file2.txt").write_text("content2")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.read_files_from_head(["file1.txt", "file2.txt", "missing.txt"])

    assert result == {"file1.txt": b"content1", "file2.txt": b"content2"}


def test_read_files_from_head_no_git_repo(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns empty dict when not in a git repo."""
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.read_files_from_head(["file.txt"])

    assert result == {}


# =============================================================================
# resolve_revision Tests
# =============================================================================


def test_resolve_revision_no_git_repo(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns None when not in a git repo."""
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.resolve_revision("HEAD")

    assert result is None


def test_resolve_revision_with_branch(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Resolves branch name to commit SHA."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("content")
    expected_sha = commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    sha = git.resolve_revision("master")
    # master might be main on some systems, try both
    if sha is None:
        sha = git.resolve_revision("main")

    assert sha is not None
    assert sha == expected_sha


def test_resolve_revision_with_short_sha(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Resolves short SHA prefix to full commit SHA."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("content")
    full_sha = commit("initial")
    short_sha = full_sha[:7]

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    sha = git.resolve_revision(short_sha)

    assert sha is not None
    assert sha == full_sha


def test_resolve_revision_invalid(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Returns None for invalid revision."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("content")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result = git.resolve_revision("nonexistent-branch")

    assert result is None


# =============================================================================
# read_file_from_revision Tests
# =============================================================================


def test_read_file_from_revision_returns_content(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    """Returns file content from specified revision."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("original content")
    first_sha = commit("first")[:7]

    # Make second commit with modified content
    (repo_path / "file.txt").write_text("modified content")
    commit("second")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    # Read from first commit
    content = git.read_file_from_revision("file.txt", first_sha)

    assert content == b"original content"


def test_read_file_from_revision_file_not_found(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    """Returns None when file doesn't exist at revision."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("content")
    sha = commit("initial")[:7]

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    content = git.read_file_from_revision("nonexistent.txt", sha)

    assert content is None


def test_read_file_from_revision_invalid_rev(git_repo: GitRepo, monkeypatch: MonkeyPatch) -> None:
    """Returns None for invalid revision."""
    repo_path, commit = git_repo
    (repo_path / "file.txt").write_text("content")
    commit("initial")

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    content = git.read_file_from_revision("file.txt", "invalid-rev")

    assert content is None


# =============================================================================
# read_files_from_revision Tests
# =============================================================================


def test_read_files_from_revision_multiple_files(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    """Returns content for multiple files from revision."""
    repo_path, commit = git_repo
    (repo_path / "file1.txt").write_text("content1")
    (repo_path / "file2.txt").write_text("content2")
    sha = commit("initial")[:7]

    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    result_files = git.read_files_from_revision(["file1.txt", "file2.txt", "missing.txt"], sha)

    assert result_files == {"file1.txt": b"content1", "file2.txt": b"content2"}


def test_read_files_from_revision_empty_list(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns empty dict for empty path list."""
    init_git_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.read_files_from_revision([], "HEAD")

    assert result == {}


# =============================================================================
# list_files_at_revision Tests
# =============================================================================


def test_list_files_at_revision_returns_files(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns list of files matching pattern in directory."""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=tmp_path, check=True, capture_output=True
    )

    # Create files in a subdirectory
    stages_dir = tmp_path / ".pivot" / "stages"
    stages_dir.mkdir(parents=True)
    (stages_dir / "stage1.lock").write_text("data1")
    (stages_dir / "stage2.lock").write_text("data2")
    (stages_dir / "other.txt").write_text("other")

    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"], cwd=tmp_path, check=True, capture_output=True
    )

    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.list_files_at_revision(".pivot/stages", "HEAD", "*.lock")

    assert sorted(result) == [".pivot/stages/stage1.lock", ".pivot/stages/stage2.lock"]


def test_list_files_at_revision_no_git_repo(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns empty list when not in a git repo."""
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.list_files_at_revision(".pivot/stages", "HEAD", "*.lock")

    assert result == []


def test_list_files_at_revision_directory_not_found(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Returns empty list when directory doesn't exist at revision."""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=tmp_path, check=True, capture_output=True
    )
    (tmp_path / "readme.txt").write_text("content")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"], cwd=tmp_path, check=True, capture_output=True
    )

    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.list_files_at_revision("nonexistent", "HEAD", "*")

    assert result == []


def test_list_files_at_revision_invalid_revision(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Returns empty list for invalid revision."""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=tmp_path, check=True, capture_output=True
    )
    (tmp_path / "readme.txt").write_text("content")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"], cwd=tmp_path, check=True, capture_output=True
    )

    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    result = git.list_files_at_revision(".pivot/stages", "invalid-rev", "*.lock")

    assert result == []


def test_list_files_at_revision_with_branch(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Lists files from a specific branch."""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=tmp_path, check=True, capture_output=True
    )

    # Create initial commit with one file
    stages_dir = tmp_path / ".pivot" / "stages"
    stages_dir.mkdir(parents=True)
    (stages_dir / "stage1.lock").write_text("data1")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "first"], cwd=tmp_path, check=True, capture_output=True)

    # Get first commit SHA
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, capture_output=True, text=True, check=True
    )
    first_sha = result.stdout.strip()[:7]

    # Add another file in second commit
    (stages_dir / "stage2.lock").write_text("data2")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "second"], cwd=tmp_path, check=True, capture_output=True)

    monkeypatch.setattr(project, "_project_root_cache", tmp_path)

    # First commit should only have stage1.lock
    result_first = git.list_files_at_revision(".pivot/stages", first_sha, "*.lock")
    assert result_first == [".pivot/stages/stage1.lock"]

    # HEAD should have both
    result_head = git.list_files_at_revision(".pivot/stages", "HEAD", "*.lock")
    assert sorted(result_head) == [".pivot/stages/stage1.lock", ".pivot/stages/stage2.lock"]


def test_read_matching_blobs_across_revisions_dedups_shared_blobs(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    repo_path, commit = git_repo
    stages_dir = repo_path / ".pivot" / "stages"
    stages_dir.mkdir(parents=True)
    (stages_dir / "a.lock").write_text("shared")
    commit("first")
    subprocess.run(["git", "branch", "dup"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "checkout", "-b", "feature"], cwd=repo_path, check=True, capture_output=True
    )
    (stages_dir / "b.lock").write_text("unique-on-feature")
    commit("second")
    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    blobs = list(
        git.read_matching_blobs_across_revisions(
            ["main", "dup", "feature"], ".pivot/stages", "*.lock"
        )
    )
    # "shared" appears on all three refs but is read once; "unique-on-feature" once.
    assert sorted(blobs) == [b"shared", b"unique-on-feature"]


def test_read_matching_blobs_across_revisions_no_git_repo(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(project, "_project_root_cache", tmp_path)
    assert list(git.read_matching_blobs_across_revisions(["main"], ".pivot/stages", "*.lock")) == []


def test_read_matching_blobs_across_revisions_ignores_submodule_gitlink(
    git_repo: GitRepo, monkeypatch: MonkeyPatch
) -> None:
    """A submodule gitlink (mode 0o160000) must not be recursed into and crash the walk.

    ``0o160000 & 0o40000 == 0o40000``, so a bitwise dir test would treat the gitlink as
    a tree and look up its commit SHA -- absent from the parent object store -- raising
    KeyError. The whole-tree ``.pvt`` scan (directory="") walks every entry, so a
    submodule anywhere would abort gc.
    """
    repo_path, commit = git_repo
    stages_dir = repo_path / ".pivot" / "stages"
    stages_dir.mkdir(parents=True)
    (stages_dir / "a.lock").write_text("lockdata")
    commit("init")
    monkeypatch.setattr(project, "_project_root_cache", repo_path)

    # Inject a gitlink entry at the root tree pointing at a commit not in this store.
    repo = dulwich.repo.Repo(str(repo_path))
    head = repo[repo.head()]
    assert isinstance(head, dulwich.objects.Commit)
    root_tree = repo[head.tree]
    assert isinstance(root_tree, dulwich.objects.Tree)
    root_tree.add(b"submodule", 0o160000, cast("dulwich.objects.ObjectID", b"0" * 40))
    repo.object_store.add_object(root_tree)
    # do_commit updates HEAD to a new commit whose tree carries the gitlink,
    # without hand-setting refs (which needs an awkward bytes->Ref cast).
    repo.do_commit(
        message=b"add gitlink",
        tree=root_tree.id,
        author=b"Test <test@test.com>",
        committer=b"Test <test@test.com>",
    )

    blobs = list(git.read_matching_blobs_across_revisions(["HEAD"], "", "*.lock"))

    assert blobs == [b"lockdata"], "gitlink ignored; real lock blob still yielded"
