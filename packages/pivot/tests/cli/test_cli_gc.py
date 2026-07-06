from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

from pivot import cli, project
from pivot import config as config_mod
from pivot import gc as gc_mod
from pivot.remote import sync as transfer
from pivot.storage import cache as cache_mod

if TYPE_CHECKING:
    import click.testing
    import pytest
    from pytest_mock import MockerFixture


def _hash(char: str) -> str:
    return char * cache_mod.XXHASH64_HEX_LENGTH


def _make_blob(cache_dir: pathlib.Path, file_hash: str, content: bytes = b"data") -> pathlib.Path:
    blob = cache_mod.get_cache_path(cache_dir / "files", file_hash)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(content)
    blob.chmod(0o444)
    return blob


def _setup(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
    *,
    referenced: set[str],
    removable: set[str],
    local_only: set[str] | None = None,
) -> pathlib.Path:
    """Wire up an isolated project + cache with mocked reference/remote resolution."""
    pathlib.Path(".pivot").mkdir()
    pathlib.Path(".git").mkdir()
    monkeypatch.setattr(project, "_project_root_cache", None)

    cache_dir = tmp_path / "cache"
    mocker.patch.object(config_mod, "get_cache_dir", return_value=cache_dir)
    mocker.patch.object(gc_mod, "collect_referenced_hashes", return_value=referenced)
    mocker.patch.object(
        transfer, "create_remote_from_name", return_value=(mocker.MagicMock(), "origin")
    )
    mocker.patch.object(
        transfer,
        "partition_local_by_remote",
        return_value=(removable, local_only or set()),
    )
    return cache_dir


def test_gc_dry_run_reports_without_deleting(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        cache_dir = _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced={_hash("a")},
            removable={_hash("b")},
        )
        _make_blob(cache_dir, _hash("a"))
        garbage = _make_blob(cache_dir, _hash("b"))

        result = runner.invoke(cli.cli, ["gc", "--dry-run"])

        assert result.exit_code == 0, result.output
        assert "Would remove 1 blob(s)" in result.output
        assert garbage.exists(), "dry-run must not delete"


def test_gc_deletes_unreferenced_blobs(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        cache_dir = _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced={_hash("a")},
            removable={_hash("b")},
        )
        kept = _make_blob(cache_dir, _hash("a"))
        garbage = _make_blob(cache_dir, _hash("b"))

        result = runner.invoke(cli.cli, ["gc", "--yes"])

        assert result.exit_code == 0, result.output
        assert "Removed 1 blob(s)" in result.output
        assert not garbage.exists(), "unreferenced blob deleted"
        assert kept.exists(), "referenced blob kept"


def test_gc_protects_local_only_blobs(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        cache_dir = _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced=set(),
            removable=set(),
            local_only={_hash("b")},
        )
        unpushed = _make_blob(cache_dir, _hash("b"))

        result = runner.invoke(cli.cli, ["gc", "--yes"])

        assert result.exit_code == 0, result.output
        assert "Keeping 1 unreferenced blob(s) not on remote" in result.output
        assert unpushed.exists(), "local-only blob is never deleted"


def test_gc_nothing_to_collect(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        cache_dir = _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced={_hash("a")},
            removable=set(),
        )
        _make_blob(cache_dir, _hash("a"))

        result = runner.invoke(cli.cli, ["gc"])

        assert result.exit_code == 0, result.output
        assert "Nothing to collect" in result.output


def test_gc_abort_on_declined_confirmation(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        cache_dir = _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced=set(),
            removable={_hash("b")},
        )
        garbage = _make_blob(cache_dir, _hash("b"))

        result = runner.invoke(cli.cli, ["gc"], input="n\n")

        assert result.exit_code != 0, "declining confirmation aborts"
        assert garbage.exists(), "nothing deleted when aborted"


def test_gc_workspace_flag_scopes_to_current_checkout(
    runner: click.testing.CliRunner,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        _setup(
            tmp_path,
            monkeypatch,
            mocker,
            referenced=set(),
            removable=set(),
        )
        spy = mocker.patch.object(gc_mod, "collect_referenced_hashes", return_value=set())

        result = runner.invoke(cli.cli, ["gc", "--workspace"])

        assert result.exit_code == 0, result.output
        spy.assert_called_once_with(gc_mod.GcScope.WORKSPACE)
