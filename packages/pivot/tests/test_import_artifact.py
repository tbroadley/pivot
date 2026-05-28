# pyright: reportMissingImports=false
from __future__ import annotations

import asyncio
import pathlib
from typing import TYPE_CHECKING

import pytest
import xxhash
import yaml

from pivot import exceptions, import_artifact
from pivot.remote import storage as remote_mod
from pivot.storage import track

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from types_aiobotocore_s3 import S3Client


def _lock_bytes(*, outs: list[dict[str, object]]) -> bytes:
    data = {
        "code_manifest": {"stage": "hash"},
        "params": {},
        "deps": [],
        "outs": outs,
    }
    return yaml.safe_dump(data).encode()


def _config_bytes(remote_url: str) -> bytes:
    data = {
        "remotes": {"origin": remote_url},
        "default_remote": "origin",
    }
    return yaml.safe_dump(data).encode()


def test_resolve_remote_path_exact_file(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": "abc123",
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    resolved = asyncio.run(
        import_artifact.resolve_remote_path(repo_url, "data/output.csv", "main", None)
    )

    assert "stage" in resolved and resolved["stage"] == "train"
    assert resolved["path"] == "data/output.csv"
    assert resolved["hash"] == "abc123"
    assert resolved["size"] == 0
    assert resolved["remote_url"] == remote_url
    assert resolved["rev_lock"] == "deadbeef"


def test_resolve_remote_path_ambiguous(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": "abc123",
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock", "eval.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path in {".pivot/stages/train.lock", ".pivot/stages/eval.lock"}:
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    with pytest.raises(exceptions.PivotError, match="train"):
        asyncio.run(import_artifact.resolve_remote_path(repo_url, "data/output.csv", "main", None))


def test_resolve_remote_path_not_found(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": "abc123",
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    with pytest.raises(exceptions.PivotError, match="Available outputs"):
        asyncio.run(import_artifact.resolve_remote_path(repo_url, "data/missing.csv", "main", None))


def test_read_remote_config_success(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.return_value = _config_bytes(remote_url)

    result = asyncio.run(import_artifact.read_remote_config(repo_url, "main", None))

    assert result == remote_url


async def test_import_artifact_creates_pvt(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    content = b"hello"
    file_hash = xxhash.xxh64(content).hexdigest()
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": file_hash,
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    key = remote_mod._hash_to_key("test-prefix/", file_hash)
    await aioboto3_s3_client.put_object(Bucket=moto_s3_bucket, Key=key, Body=content)

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    result = await import_artifact.import_artifact(
        repo_url,
        "data/output.csv",
        rev="main",
        project_root=tmp_path,
    )

    data_path = tmp_path / "data" / "output.csv"
    pvt_path = track.get_pvt_path(data_path)
    assert pathlib.Path(result["data_path"]) == data_path
    assert pathlib.Path(result["pvt_path"]) == pvt_path
    assert result["downloaded"] is True
    assert data_path.read_bytes() == content
    pvt_data = track.read_pvt_file(pvt_path)
    assert pvt_data is not None
    assert pvt_data["hash"] == file_hash
    assert "source" in pvt_data
    assert pvt_data["source"]["remote"] == remote_url
    assert pvt_data["size"] == len(content), "Size should come from downloaded file"


def test_import_artifact_no_download(tmp_path: pathlib.Path, mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"
    content = b"hello"
    file_hash = xxhash.xxh64(content).hexdigest()
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": file_hash,
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        side_effect=AssertionError("S3Remote should not be instantiated"),
    )

    result = asyncio.run(
        import_artifact.import_artifact(
            repo_url,
            "data/output.csv",
            rev="main",
            project_root=tmp_path,
            no_download=True,
        )
    )

    data_path = tmp_path / "data" / "output.csv"
    pvt_path = track.get_pvt_path(data_path)
    assert pathlib.Path(result["data_path"]) == data_path
    assert pathlib.Path(result["pvt_path"]) == pvt_path
    assert result["downloaded"] is False
    assert not data_path.exists()
    assert pvt_path.exists()


async def test_import_artifact_force_overwrites(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    content = b"new"
    file_hash = xxhash.xxh64(content).hexdigest()
    lock_bytes = _lock_bytes(
        outs=[
            {
                "path": "data/output.csv",
                "hash": file_hash,
            }
        ]
    )

    list_directory = mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    list_directory.return_value = ["train.lock"]
    resolve_ref = mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
    )
    resolve_ref.return_value = "deadbeef"

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return lock_bytes
        raise AssertionError(f"Unexpected path: {path}")

    read_file = mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
    )
    read_file.side_effect = _read_file

    key = remote_mod._hash_to_key("test-prefix/", file_hash)
    await aioboto3_s3_client.put_object(Bucket=moto_s3_bucket, Key=key, Body=content)

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    data_path = tmp_path / "data" / "output.csv"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_text("old")
    track.write_pvt_file(
        track.get_pvt_path(data_path),
        track.PvtData(path="output.csv", hash="old", size=3),
    )

    result = await import_artifact.import_artifact(
        repo_url,
        "data/output.csv",
        rev="main",
        project_root=tmp_path,
        force=True,
    )

    assert pathlib.Path(result["data_path"]) == data_path
    assert data_path.read_bytes() == content
    pvt_path = track.get_pvt_path(data_path)
    pvt_data = track.read_pvt_file(pvt_path)
    assert pvt_data is not None
    assert pvt_data["hash"] == file_hash
    assert pvt_data["size"] == len(content), "Size should come from downloaded file"


# ── Tracked-file (.pvt) resolution ────────────────────────────


def _pvt_bytes(
    *, path: str, hash: str, size: int, manifest: list[dict[str, object]] | None = None
) -> bytes:
    data: dict[str, object] = {"path": path, "hash": hash, "size": size}
    if manifest is not None:
        data["manifest"] = manifest
    return yaml.safe_dump(data).encode()


def test_resolve_remote_path_tracked_pvt_alongside_lock(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=["data/foo.csv.pvt", "data/other.csv.pvt"],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "data/from_stage.csv", "hash": "stagehash"}])
        if path == "data/foo.csv.pvt":
            return _pvt_bytes(path="foo.csv", hash="abc123", size=42)
        if path == "data/other.csv.pvt":
            return _pvt_bytes(path="other.csv", hash="otherhash", size=7)
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    resolved = asyncio.run(
        import_artifact.resolve_remote_path(repo_url, "data/foo.csv", "main", None)
    )

    assert resolved["path"] == "data/foo.csv"
    assert resolved["hash"] == "abc123"
    assert resolved["size"] == 42
    assert resolved["remote_url"] == remote_url
    assert "stage" not in resolved, "Tracked-file imports should not record a stage"


def test_resolve_remote_path_tracked_pvt_manifest_entry(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=["images.pvt"],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    manifest: list[dict[str, object]] = [
        {"relpath": "a.png", "hash": "ah", "size": 10, "isexec": False},
        {"relpath": "b.png", "hash": "bh", "size": 20, "isexec": False},
    ]

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "unrelated.csv", "hash": "x"}])
        if path == "images.pvt":
            return _pvt_bytes(path="images", hash="dirhash", size=30, manifest=manifest)
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    resolved = asyncio.run(
        import_artifact.resolve_remote_path(repo_url, "images/a.png", "main", None)
    )

    assert resolved["hash"] == "ah"
    assert resolved["size"] == 10
    assert "stage" not in resolved


def test_resolve_remote_path_tracked_pvt_directory(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=["images.pvt"],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    manifest: list[dict[str, object]] = [
        {"relpath": "a.png", "hash": "ah", "size": 10, "isexec": False},
        {"relpath": "b.png", "hash": "bh", "size": 20, "isexec": False},
    ]

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "unrelated.csv", "hash": "x"}])
        if path == "images.pvt":
            return _pvt_bytes(path="images", hash="dirhash", size=30, manifest=manifest)
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    resolved = asyncio.run(import_artifact.resolve_remote_path(repo_url, "images", "main", None))

    assert resolved["path"] == "images"
    assert resolved["hash"] == "dirhash"
    assert resolved["size"] == 30
    assert "manifest" in resolved and resolved["manifest"] == manifest
    assert "stage" not in resolved


def test_resolve_remote_path_lock_pvt_collision(mocker: MockerFixture) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=["data/foo.csv.pvt"],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "data/foo.csv", "hash": "stagehash"}])
        if path == "data/foo.csv.pvt":
            return _pvt_bytes(path="foo.csv", hash="pvthash", size=5)
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    with pytest.raises(exceptions.PivotError, match="multiple sources"):
        asyncio.run(import_artifact.resolve_remote_path(repo_url, "data/foo.csv", "main", None))


def test_resolve_remote_path_tracked_only_no_stages(mocker: MockerFixture) -> None:
    """Repo with only `pivot track`-ed files and no stages should still resolve."""
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/prefix"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=None,  # .pivot/stages does not exist
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=["data/foo.csv.pvt"],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == "data/foo.csv.pvt":
            return _pvt_bytes(path="foo.csv", hash="abc123", size=42)
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    resolved = asyncio.run(
        import_artifact.resolve_remote_path(repo_url, "data/foo.csv", "main", None)
    )

    assert resolved["hash"] == "abc123"
    assert "stage" not in resolved


# ── Directory imports ────────────────────────────────────────


def _dir_manifest_with_content(
    files: dict[str, bytes],
) -> tuple[list[dict[str, object]], dict[str, bytes], str, int]:
    """Build a manifest from a {relpath: content} mapping.

    Returns (manifest, {hash: content}, tree_hash, total_size).
    """
    import json

    manifest: list[dict[str, object]] = []
    blobs: dict[str, bytes] = {}
    for relpath in sorted(files):
        content = files[relpath]
        h = xxhash.xxh64(content).hexdigest()
        manifest.append({"relpath": relpath, "hash": h, "size": len(content), "isexec": False})
        blobs[h] = content
    manifest_json = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    tree_hash = xxhash.xxh64(manifest_json.encode()).hexdigest()
    total_size = sum(len(c) for c in files.values())
    return manifest, blobs, tree_hash, total_size


def _patch_github_for_dir_pvt(
    mocker: MockerFixture,
    *,
    remote_url: str,
    pvt_remote_path: str,
    pvt_path_value: str,
    tree_hash: str,
    total_size: int,
    manifest: list[dict[str, object]],
) -> None:
    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=None,
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[pvt_remote_path],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == pvt_remote_path:
            return _pvt_bytes(
                path=pvt_path_value, hash=tree_hash, size=total_size, manifest=manifest
            )
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )


async def test_import_artifact_directory_creates_pvt_with_manifest(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    files = {"a.txt": b"alpha", "sub/b.txt": b"beta"}
    manifest, blobs, tree_hash, total_size = _dir_manifest_with_content(files)

    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="images.pvt",
        pvt_path_value="images",
        tree_hash=tree_hash,
        total_size=total_size,
        manifest=manifest,
    )

    for h, content in blobs.items():
        key = remote_mod._hash_to_key("test-prefix/", h)
        await aioboto3_s3_client.put_object(Bucket=moto_s3_bucket, Key=key, Body=content)

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    result = await import_artifact.import_artifact(
        repo_url,
        "images",
        rev="main",
        project_root=tmp_path,
        cache_dir=tmp_path / "cache",
    )

    data_path = tmp_path / "images"
    pvt_path = track.get_pvt_path(data_path)
    assert pathlib.Path(result["data_path"]) == data_path
    assert pathlib.Path(result["pvt_path"]) == pvt_path
    assert result["downloaded"] is True
    assert data_path.is_dir()
    assert (data_path / "a.txt").read_bytes() == b"alpha"
    assert (data_path / "sub" / "b.txt").read_bytes() == b"beta"

    pvt_data = track.read_pvt_file(pvt_path)
    assert pvt_data is not None
    assert pvt_data["hash"] == tree_hash
    assert pvt_data["size"] == total_size
    assert "num_files" in pvt_data and pvt_data["num_files"] == 2
    assert "manifest" in pvt_data and pvt_data["manifest"] == manifest
    assert "source" in pvt_data
    assert pvt_data["source"]["path"] == "images"
    assert pvt_data["source"]["repo"] == repo_url


async def test_import_artifact_directory_from_lockfile(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    files = {"foo.bin": b"FOO", "bar.bin": b"BARS"}
    manifest, blobs, tree_hash, total_size = _dir_manifest_with_content(files)

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "data/raw", "hash": tree_hash, "manifest": manifest}])
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    for h, content in blobs.items():
        key = remote_mod._hash_to_key("test-prefix/", h)
        await aioboto3_s3_client.put_object(Bucket=moto_s3_bucket, Key=key, Body=content)

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    result = await import_artifact.import_artifact(
        repo_url,
        "data/raw",
        rev="main",
        project_root=tmp_path,
        cache_dir=tmp_path / "cache",
    )

    data_path = tmp_path / "data" / "raw"
    assert pathlib.Path(result["data_path"]) == data_path
    assert (data_path / "foo.bin").read_bytes() == b"FOO"
    assert (data_path / "bar.bin").read_bytes() == b"BARS"

    pvt_data = track.read_pvt_file(track.get_pvt_path(data_path))
    assert pvt_data is not None
    assert "source" in pvt_data
    assert "stage" in pvt_data["source"] and pvt_data["source"]["stage"] == "train"
    assert "manifest" in pvt_data and pvt_data["manifest"] == manifest


async def test_import_artifact_directory_cache_skip(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
) -> None:
    """When all blobs are in cache, no S3 download is attempted."""
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    files = {"a.txt": b"alpha", "b.txt": b"beta"}
    manifest, blobs, tree_hash, total_size = _dir_manifest_with_content(files)

    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="images.pvt",
        pvt_path_value="images",
        tree_hash=tree_hash,
        total_size=total_size,
        manifest=manifest,
    )

    cache_dir = tmp_path / "cache"
    for h, content in blobs.items():
        cache_path = cache_dir / h[:2] / h[2:]
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)

    s3_mock = mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
    )

    result = await import_artifact.import_artifact(
        repo_url,
        "images",
        rev="main",
        project_root=tmp_path,
        cache_dir=cache_dir,
    )

    assert result["downloaded"] is True
    data_path = tmp_path / "images"
    assert (data_path / "a.txt").read_bytes() == b"alpha"
    assert (data_path / "b.txt").read_bytes() == b"beta"
    s3_mock.assert_not_called()


async def test_import_artifact_directory_hash_mismatch_aborts(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    """If a downloaded blob fails hash verification, no .pvt is written."""
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"
    files = {"good.txt": b"good", "bad.txt": b"expected"}
    manifest, _blobs, tree_hash, total_size = _dir_manifest_with_content(files)

    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="images.pvt",
        pvt_path_value="images",
        tree_hash=tree_hash,
        total_size=total_size,
        manifest=manifest,
    )

    # Upload good content under the real hash, but upload CORRUPTED bytes under
    # bad.txt's hash to simulate corruption.
    good_hash = xxhash.xxh64(b"good").hexdigest()
    bad_hash = xxhash.xxh64(b"expected").hexdigest()
    await aioboto3_s3_client.put_object(
        Bucket=moto_s3_bucket,
        Key=remote_mod._hash_to_key("test-prefix/", good_hash),
        Body=b"good",
    )
    await aioboto3_s3_client.put_object(
        Bucket=moto_s3_bucket,
        Key=remote_mod._hash_to_key("test-prefix/", bad_hash),
        Body=b"corrupted",
    )

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    with pytest.raises(exceptions.RemoteError, match="Hash mismatch"):
        await import_artifact.import_artifact(
            repo_url,
            "images",
            rev="main",
            project_root=tmp_path,
            cache_dir=tmp_path / "cache",
        )

    data_path = tmp_path / "images"
    pvt_path = track.get_pvt_path(data_path)
    assert not data_path.exists(), "Staging should be cleaned up on failure"
    assert not pvt_path.exists(), "No .pvt should be written on failure"
    leftover = list(tmp_path.glob(".images.import-*"))
    assert leftover == [], f"Staging dir should be removed, found: {leftover}"


def _write_existing_import_pvt(
    project_root: pathlib.Path,
    *,
    rel_data_path: str,
    pvt_path_field: str,
    source_repo: str,
    source_path: str,
) -> None:
    data_path = project_root / rel_data_path
    data_path.parent.mkdir(parents=True, exist_ok=True)
    pvt_path = track.get_pvt_path(data_path)
    track.write_pvt_file(
        pvt_path,
        track.PvtData(
            path=pvt_path_field,
            hash="aa" * 8,
            size=1,
            source=track.ImportSource(
                repo=source_repo,
                rev="main",
                rev_lock="deadbeef",
                path=source_path,
                remote="s3://bucket/prefix",
            ),
        ),
    )


async def test_import_artifact_conflict_directory_over_existing_file(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
) -> None:
    """Importing dir 'data/raw' fails when 'data/raw/foo.csv' is already imported."""
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/test-prefix/"
    files = {"foo.csv": b"X"}
    manifest, _blobs, tree_hash, total_size = _dir_manifest_with_content(files)

    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="data/raw.pvt",
        pvt_path_value="raw",
        tree_hash=tree_hash,
        total_size=total_size,
        manifest=manifest,
    )

    _write_existing_import_pvt(
        tmp_path,
        rel_data_path="data/raw/foo.csv",
        pvt_path_field="foo.csv",
        source_repo=repo_url,
        source_path="data/raw/foo.csv",
    )

    with pytest.raises(exceptions.PivotError, match="Import conflict"):
        await import_artifact.import_artifact(
            repo_url,
            "data/raw",
            rev="main",
            project_root=tmp_path,
            cache_dir=tmp_path / "cache",
        )


async def test_import_artifact_conflict_file_under_existing_directory(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
) -> None:
    """Importing file 'data/raw/foo.csv' fails when 'data/raw' is already imported as dir."""
    repo_url = "https://github.com/org/repo"
    remote_url = "s3://bucket/test-prefix/"

    mocker.patch(
        "pivot.import_artifact.github.list_directory",
        autospec=True,
        return_value=["train.lock"],
    )
    mocker.patch(
        "pivot.import_artifact.github.list_tree",
        autospec=True,
        return_value=[],
    )
    mocker.patch(
        "pivot.import_artifact.github.resolve_ref",
        autospec=True,
        return_value="deadbeef",
    )

    async def _read_file(
        _owner: str, _repo: str, path: str, _ref: str, _token: str | None = None, **_kw: object
    ) -> bytes | None:
        if path == ".pivot/config.yaml":
            return _config_bytes(remote_url)
        if path == ".pivot/stages/train.lock":
            return _lock_bytes(outs=[{"path": "data/raw/foo.csv", "hash": "abc"}])
        raise AssertionError(f"Unexpected path: {path}")

    mocker.patch(
        "pivot.import_artifact.github.read_file",
        autospec=True,
        side_effect=_read_file,
    )

    # Existing directory import covering data/raw.
    _write_existing_import_pvt(
        tmp_path,
        rel_data_path="data/raw",  # data path is the directory itself
        pvt_path_field="raw",
        source_repo=repo_url,
        source_path="data/raw",
    )
    # Make the dir actually exist so we don't trip data_path.exists() check first
    (tmp_path / "data" / "raw").mkdir(parents=True, exist_ok=True)

    with pytest.raises(exceptions.PivotError, match="Import conflict"):
        await import_artifact.import_artifact(
            repo_url,
            "data/raw/foo.csv",
            rev="main",
            project_root=tmp_path,
            cache_dir=tmp_path / "cache",
        )


async def test_update_import_directory_delta_only_fetches_changed_blob(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    moto_s3_bucket: str,
    aioboto3_s3_client: S3Client,
) -> None:
    """Updating a directory import re-uses cached blobs and only fetches changed ones."""
    repo_url = "https://github.com/org/repo"
    remote_url = f"s3://{moto_s3_bucket}/test-prefix/"

    files_v1 = {"a.txt": b"alpha", "b.txt": b"beta"}
    manifest_v1, blobs_v1, tree_v1, size_v1 = _dir_manifest_with_content(files_v1)

    files_v2 = {"a.txt": b"alpha", "b.txt": b"BETA-CHANGED"}
    manifest_v2, blobs_v2, tree_v2, size_v2 = _dir_manifest_with_content(files_v2)

    # ── Stage 1: import v1 ────────────────────────────────────
    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="data.pvt",
        pvt_path_value="data",
        tree_hash=tree_v1,
        total_size=size_v1,
        manifest=manifest_v1,
    )

    for h, content in blobs_v1.items():
        key = remote_mod._hash_to_key("test-prefix/", h)
        await aioboto3_s3_client.put_object(Bucket=moto_s3_bucket, Key=key, Body=content)

    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=remote_mod.S3Remote(remote_url),
    )

    await import_artifact.import_artifact(
        repo_url,
        "data",
        rev="main",
        project_root=tmp_path,
        cache_dir=tmp_path / "cache",
    )

    data_path = tmp_path / "data"
    assert (data_path / "b.txt").read_bytes() == b"beta"

    # ── Stage 2: switch source to v2 (b.txt changed), update ──
    mocker.stopall()
    _patch_github_for_dir_pvt(
        mocker,
        remote_url=remote_url,
        pvt_remote_path="data.pvt",
        pvt_path_value="data",
        tree_hash=tree_v2,
        total_size=size_v2,
        manifest=manifest_v2,
    )

    new_b_hash = next(h for h, c in blobs_v2.items() if c == b"BETA-CHANGED")
    await aioboto3_s3_client.put_object(
        Bucket=moto_s3_bucket,
        Key=remote_mod._hash_to_key("test-prefix/", new_b_hash),
        Body=b"BETA-CHANGED",
    )

    # Track which hashes get downloaded by wrapping the real S3Remote.
    real_s3 = remote_mod.S3Remote(remote_url)
    download_calls: list[list[str]] = []
    real_download_batch = real_s3.download_batch

    async def _spy_batch(items, *args, **kwargs):  # pyright: ignore[reportMissingParameterType,reportUnknownParameterType]
        download_calls.append([h for h, _ in items])
        return await real_download_batch(items, *args, **kwargs)  # pyright: ignore[reportUnknownArgumentType]

    real_s3.download_batch = _spy_batch  # type: ignore[method-assign]
    mocker.patch(
        "pivot.import_artifact.remote_storage.S3Remote",
        autospec=True,
        return_value=real_s3,
    )

    pvt_path = track.get_pvt_path(data_path)
    result = await import_artifact.update_import(pvt_path, cache_dir=tmp_path / "cache")

    assert result["downloaded"] is True
    assert (data_path / "a.txt").read_bytes() == b"alpha"
    assert (data_path / "b.txt").read_bytes() == b"BETA-CHANGED"

    assert len(download_calls) == 1
    assert download_calls[0] == [new_b_hash], (
        f"Only the changed blob should be fetched; got {download_calls[0]}"
    )

    pvt_data = track.read_pvt_file(pvt_path)
    assert pvt_data is not None
    assert pvt_data["hash"] == tree_v2
    assert "manifest" in pvt_data and pvt_data["manifest"] == manifest_v2
