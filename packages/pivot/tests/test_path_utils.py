"""Tests for path_utils module."""

import os
from pathlib import Path

from pivot import path_utils


def test_preserve_trailing_slash_with_slash() -> None:
    assert path_utils.preserve_trailing_slash("foo/", "foo") == "foo/"


def test_preserve_trailing_slash_without_slash() -> None:
    assert path_utils.preserve_trailing_slash("foo", "foo") == "foo"


def test_preserve_trailing_slash_already_has_slash() -> None:
    assert path_utils.preserve_trailing_slash("foo/", "foo/") == "foo/"


def test_preserve_trailing_slash_with_double_slash() -> None:
    """preserve_trailing_slash handles paths with multiple trailing slashes."""
    # Should preserve single slash even if original has multiple
    assert path_utils.preserve_trailing_slash("foo//", "foo") == "foo/"


def test_preserve_trailing_slash_normalized_empty() -> None:
    """preserve_trailing_slash handles empty normalized path."""
    # Empty normalized should get slash if original had it
    assert path_utils.preserve_trailing_slash("foo/", "") == "/"


def test_preserve_trailing_slash_both_empty() -> None:
    """preserve_trailing_slash handles both paths being empty."""
    assert path_utils.preserve_trailing_slash("", "") == ""


def test_preserve_trailing_slash_original_slash_normalized_has_slash() -> None:
    """preserve_trailing_slash is idempotent when normalized already has slash."""
    # If normalized already has slash, don't add another
    assert path_utils.preserve_trailing_slash("bar/", "foo/") == "foo/"


def test_canonicalize_artifact_path_relative(tmp_path: Path) -> None:
    """Relative path becomes absolute from base."""
    result = path_utils.canonicalize_artifact_path("data/input.csv", tmp_path)
    assert result == str(tmp_path / "data" / "input.csv")
    assert os.path.isabs(result)


def test_canonicalize_artifact_path_absolute(tmp_path: Path) -> None:
    """Absolute path stays absolute, gets normalized."""
    abs_input = str(tmp_path / "data" / ".." / "data" / "input.csv")
    result = path_utils.canonicalize_artifact_path(abs_input, tmp_path)
    assert result == str(tmp_path / "data" / "input.csv")


def test_canonicalize_artifact_path_trailing_slash(tmp_path: Path) -> None:
    """Trailing slash preserved for directory paths."""
    result = path_utils.canonicalize_artifact_path("outputs/", tmp_path)
    assert result.endswith("/")
    assert result == str(tmp_path / "outputs") + "/"


def test_canonicalize_artifact_path_no_trailing_slash(tmp_path: Path) -> None:
    """Non-directory paths don't get trailing slash."""
    result = path_utils.canonicalize_artifact_path("data/input.csv", tmp_path)
    assert not result.endswith("/")


def test_canonicalize_artifact_path_dotdot_normalized(tmp_path: Path) -> None:
    """Parent traversal is collapsed."""
    result = path_utils.canonicalize_artifact_path("sub/../data/input.csv", tmp_path)
    assert result == str(tmp_path / "data" / "input.csv")


def test_make_exclude_matcher_empty_returns_none() -> None:
    """No patterns (or slash-only patterns) yields no matcher."""
    assert path_utils.make_exclude_matcher([]) is None
    assert path_utils.make_exclude_matcher(["/", "///"]) is None


def test_make_exclude_matcher_directory_prefix_and_exact() -> None:
    """A pattern matches an exact path or any path nested under it."""
    matcher = path_utils.make_exclude_matcher(["data/raw/sensitive"])
    assert matcher is not None
    assert matcher("data/raw/sensitive")
    assert matcher("data/raw/sensitive/scans")
    assert matcher("data/raw/sensitive/scans/run1.json")


def test_make_exclude_matcher_does_not_match_sibling_prefix() -> None:
    """A name-prefix sibling is not excluded."""
    matcher = path_utils.make_exclude_matcher(["data/raw/sensitive"])
    assert matcher is not None
    assert not matcher("data/raw/sensitive2")
    assert not matcher("data/raw/sensitive2/x.csv")
    assert not matcher("data/raw/public")


def test_make_exclude_matcher_strips_surrounding_slashes() -> None:
    """Leading/trailing slashes on patterns and paths are ignored."""
    matcher = path_utils.make_exclude_matcher(["/data/raw/sensitive/"])
    assert matcher is not None
    assert matcher("data/raw/sensitive/scans/")


def test_make_exclude_matcher_multiple_patterns() -> None:
    """Any matching pattern excludes the path."""
    matcher = path_utils.make_exclude_matcher(["a/b", "c/d"])
    assert matcher is not None
    assert matcher("a/b/x")
    assert matcher("c/d")
    assert not matcher("e/f")
