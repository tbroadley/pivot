from __future__ import annotations

import asyncio
import contextlib
import enum
import pathlib
from typing import TYPE_CHECKING, Literal

import click

from pivot import config, path_utils, project, registry
from pivot.cli import completion
from pivot.cli import decorators as cli_decorators
from pivot.cli import helpers as cli_helpers
from pivot.storage import cache, lock, state, track
from pivot.types import HashInfo, is_dir_hash

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

RestoreResult = Literal["restored", "skipped", "missing"]
MAX_CONCURRENT_RESTORES = 32


class CheckoutBehavior(enum.StrEnum):
    """How to handle existing files during checkout."""

    SAFE = "safe"  # Update stale (cached) versions; error on untracked changes (default)
    SKIP_EXISTING = "skip_existing"  # Only restore missing files/dir entries (--only-missing)
    FORCE = "force"  # Overwrite existing files unconditionally (--force)


OnDiskClass = Literal["matches", "known", "untracked"]


def _get_stage_output_info() -> dict[str, HashInfo]:
    """Get output hash info from lock files for cached stage outputs only.

    Non-cached outputs (e.g. Metric with cache=False) are excluded —
    they are git-tracked and not Pivot's responsibility to restore.

    Uses per-stage state_dir from the registry for lock file lookup.
    """
    result = dict[str, HashInfo]()

    for stage_name in cli_helpers.list_stages():
        stage_info = cli_helpers.get_stage(stage_name)
        project_root = project.get_project_root()
        cached_paths = {
            path_utils.canonicalize_artifact_path(str(out.path), project_root)
            for out in stage_info["outs"]
            if out.cache
        }

        stage_state_dir = registry.get_stage_state_dir(stage_info, config.get_state_dir())
        stage_lock = lock.StageLock(stage_name, lock.get_stages_dir(stage_state_dir))
        lock_data = stage_lock.read()
        if lock_data:
            for out_path, out_hash in lock_data["output_hashes"].items():
                norm_path = path_utils.canonicalize_artifact_path(out_path, project_root)
                if norm_path in cached_paths:
                    result[norm_path] = out_hash

    return result


def _classify_existing(
    path: pathlib.Path,
    output_hash: HashInfo,
    cache_dir: pathlib.Path,
    state_db: state.StateDB | None,
) -> OnDiskClass:
    """Classify existing on-disk content relative to the target and the cache.

    - "matches": on-disk content already equals the target hash (nothing to do).
    - "known": content differs from the target but every byte is present in the
      cache, so it is a prior Pivot-tracked version (e.g. a stale checkout left
      behind after `git pull`) -- safe to overwrite.
    - "untracked": content has bytes Pivot never stored (local edits, or extra
      files inside a directory) -- overwriting would lose data.

    File hashes go through the StateDB cache, so an unchanged file costs an O(1)
    stat; content is only re-read when its mtime/size/inode changed.
    """
    if path.is_dir():
        tree_hash, manifest = cache.hash_directory(path, state_db)
        if is_dir_hash(output_hash) and tree_hash == output_hash["hash"]:
            return "matches"
        if all(cache.get_cache_path(cache_dir, entry["hash"]).exists() for entry in manifest):
            return "known"
        return "untracked"

    file_hash, _ = cache.hash_file(path, state_db)
    if not is_dir_hash(output_hash) and file_hash == output_hash["hash"]:
        return "matches"
    if cache.get_cache_path(cache_dir, file_hash).exists():
        return "known"
    return "untracked"


def _restore_path_sync(
    path: pathlib.Path,
    output_hash: HashInfo,
    cache_dir: pathlib.Path,
    checkout_modes: list[cache.CheckoutMode],
    behavior: CheckoutBehavior,
    state_dir: pathlib.Path | None = None,
    state_db: state.StateDB | None = None,
) -> tuple[RestoreResult, str]:
    """Restore a file or directory from cache (sync version).

    Returns:
        Tuple of (result, path_name) for the caller to handle output.

    Raises:
        click.ClickException: For immediate failures (path traversal, unknown target,
            untracked local changes without --force). Cache misses return
            ("missing", name) instead of raising.
    """
    if path.exists():
        match behavior:
            case CheckoutBehavior.SAFE:
                match _classify_existing(path, output_hash, cache_dir, state_db):
                    case "matches":
                        return ("skipped", path.name)
                    case "untracked":
                        raise click.ClickException(
                            f"'{path.name}' already exists with local changes that Pivot "
                            + "has not stored; refusing to overwrite and lose data. "
                            + "Use --force to overwrite or --only-missing to skip existing files."
                        )
                    case "known":
                        # Stale but cache-backed version - safe to overwrite below.
                        pass
            case CheckoutBehavior.SKIP_EXISTING:
                # Existing file: skip. Existing directory: fill in only the missing
                # inner files, never overwriting existing (possibly modified) ones.
                if not is_dir_hash(output_hash) or not path.is_dir():
                    return ("skipped", path.name)
                restored, unavailable = cache.restore_missing_in_directory(
                    path, output_hash, cache_dir, checkout_modes
                )
                if unavailable:
                    return ("missing", path.name)
                return ("restored" if restored else "skipped", path.name)
            case CheckoutBehavior.FORCE:
                cache.remove_output(path)
            case _:  # pyright: ignore[reportUnnecessaryComparison] - defensive for future enum values
                raise ValueError(f"Unhandled checkout behavior: {behavior}")  # pyright: ignore[reportUnreachable]

    success = cache.restore_from_cache(
        path,
        output_hash,
        cache_dir,
        checkout_modes=checkout_modes,
        state_dir=state_dir,
    )
    if not success:
        return ("missing", path.name)

    return ("restored", path.name)


async def _checkout_files_async(
    files: Mapping[str, HashInfo],
    cache_dir: pathlib.Path,
    checkout_modes: list[cache.CheckoutMode],
    behavior: CheckoutBehavior,
    callback: Callable[[int, int, str], None] | None = None,
    state_dir: pathlib.Path | None = None,
    state_db: state.StateDB | None = None,
) -> tuple[list[str], int, int]:
    """Restore files in parallel.

    Returns:
        Tuple of (failures, restored_count, skipped_count) where failures is a list
        of file names that were missing from cache.

    Raises:
        click.ClickException: For immediate failures (aggregated if multiple).
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_RESTORES)
    failures = list[str]()
    restored = 0
    skipped = 0
    immediate_errors = list[click.ClickException]()

    async def restore_one(abs_path_str: str, output_hash: HashInfo) -> None:
        nonlocal restored, skipped
        path = pathlib.Path(abs_path_str)
        try:
            async with semaphore:
                result, name = await asyncio.to_thread(
                    _restore_path_sync,
                    path,
                    output_hash,
                    cache_dir,
                    checkout_modes,
                    behavior,
                    state_dir,
                    state_db,
                )
            match result:
                case "missing":
                    failures.append(name)
                case "restored":
                    restored += 1
                case "skipped":
                    skipped += 1
            if callback:
                callback(restored + skipped + len(failures), len(files), name)
        except click.ClickException as e:
            immediate_errors.append(e)

    try:
        async with asyncio.TaskGroup() as tg:
            for abs_path_str, output_hash in files.items():
                tg.create_task(restore_one(abs_path_str, output_hash))
    except* Exception as eg:
        # Convert unexpected exceptions to friendly error message
        errors = [str(e) for e in eg.exceptions]
        raise click.ClickException("\n".join(errors)) from None

    # Re-raise immediate failures (all of them, aggregated)
    if immediate_errors:
        msgs = [str(e.message) for e in immediate_errors]
        raise click.ClickException("\n".join(msgs))

    return (failures, restored, skipped)


def _dedupe_targets(targets: tuple[str, ...]) -> list[str]:
    """Deduplicate targets by normalized absolute path.

    Handles both data paths (data.txt) and .pvt paths (data.txt.pvt) resolving
    to the same file.
    """
    seen = set[str]()
    unique = list[str]()
    for target in targets:
        # Convert .pvt to data path
        target_path = pathlib.Path(target)
        if target_path.suffix == ".pvt":
            target = str(track.get_data_path(target_path))

        abs_path = str(project.normalize_path(target))
        if abs_path not in seen:
            seen.add(abs_path)
            unique.append(target)
    return unique


def _validate_and_build_files(
    targets: list[str],
    tracked_files: dict[str, track.PvtData],
    stage_outputs: dict[str, HashInfo],
) -> dict[str, HashInfo]:
    """Validate targets and build files dict for checkout.

    Targets should already have .pvt suffixes converted by _dedupe_targets().

    Raises:
        click.ClickException: For path traversal or unknown targets.
    """
    files = dict[str, HashInfo]()

    for target in targets:
        # Use normalized path (preserve symlinks) to match keys in tracked_files/stage_outputs
        abs_path = project.normalize_path(target)
        # Validate path is within project root (replaces literal ".." check)
        if not abs_path.is_relative_to(project.get_project_root()):
            raise click.ClickException(f"Target '{target}' resolves outside project root")
        abs_path_str = str(abs_path)

        # Check if it's a tracked file
        if abs_path_str in tracked_files:
            pvt_data = tracked_files[abs_path_str]
            files[abs_path_str] = track.pvt_to_hash_info(pvt_data)
            continue

        # Check if it's a stage output
        if abs_path_str in stage_outputs:
            files[abs_path_str] = stage_outputs[abs_path_str]
            continue

        # Unknown target
        raise click.ClickException(
            f"'{target}' is not a tracked file or stage output. "
            + "Use 'pivot list' to see stages or 'pivot track' to track files."
        )

    return files


async def _checkout_main_async(
    targets: tuple[str, ...],
    tracked_files: dict[str, track.PvtData],
    stage_outputs: dict[str, HashInfo],
    cache_dir: pathlib.Path,
    checkout_modes: list[cache.CheckoutMode],
    behavior: CheckoutBehavior,
    callback: Callable[[int, int, str], None] | None = None,
    state_dir: pathlib.Path | None = None,
    state_db: state.StateDB | None = None,
) -> tuple[list[str], int, int]:
    """Main async checkout logic.

    Returns:
        Tuple of (failures, restored_count, skipped_count).
    """
    if targets:
        unique_targets = _dedupe_targets(targets)
        files = _validate_and_build_files(unique_targets, tracked_files, stage_outputs)
        return await _checkout_files_async(
            files,
            cache_dir,
            checkout_modes,
            behavior,
            callback,
            state_dir=state_dir,
            state_db=state_db,
        )
    else:
        # Checkout all tracked files and stage outputs
        tracked_as_hashes: dict[str, HashInfo] = {
            path: track.pvt_to_hash_info(pvt) for path, pvt in tracked_files.items()
        }
        combined_total = len(tracked_as_hashes) + len(stage_outputs)
        shared_completed = 0

        def shared_callback(_completed: int, _total: int, filename: str) -> None:
            nonlocal shared_completed
            shared_completed += 1
            if callback:
                callback(shared_completed, combined_total, filename)

        progress_cb = shared_callback if callback else None
        # Run both in parallel with shared progress counter
        t1 = asyncio.create_task(
            _checkout_files_async(
                tracked_as_hashes,
                cache_dir,
                checkout_modes,
                behavior,
                progress_cb,
                state_dir=state_dir,
                state_db=state_db,
            )
        )
        t2 = asyncio.create_task(
            _checkout_files_async(
                stage_outputs,
                cache_dir,
                checkout_modes,
                behavior,
                progress_cb,
                state_dir=state_dir,
                state_db=state_db,
            )
        )
        (f1, r1, s1), (f2, r2, s2) = await asyncio.gather(t1, t2)
        return (f1 + f2, r1 + r2, s1 + s2)


def _print_summary(failures: list[str], restored: int, skipped: int, quiet: bool) -> bool:
    """Print checkout summary.

    Returns:
        True if all files were restored successfully, False if any were missing.
    """
    if not quiet and (restored or skipped):
        if restored:
            click.echo(f"Restored {restored} file(s)")
        if skipped:
            click.echo(f"Skipped {skipped} file(s) (already exist)")

    if failures:
        if restored or skipped:
            click.echo("")  # Add blank line after success summary
        click.echo(f"Missing {len(failures)} file(s):")
        for name in failures[:15]:
            click.echo(f"  {name}")
        if len(failures) > 15:
            click.echo(f"  ... and {len(failures) - 15} more")
        click.echo("")
        click.echo("Run 'pivot pull' to fetch from remote storage.")
        return False

    return True


@cli_decorators.pivot_command(allow_all=True)
@click.argument("targets", nargs=-1, shell_complete=completion.complete_targets)
@click.option(
    "--checkout-mode",
    type=click.Choice(["symlink", "hardlink", "copy"]),
    default=None,
    help="Checkout mode for restoration (default: project config or hardlink)",
)
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
@click.option(
    "--only-missing",
    is_flag=True,
    help="Only restore files that don't exist on disk (safe for local modifications)",
)
@click.option(
    "--exclude",
    "exclude",
    multiple=True,
    help="Exclude paths matching PATTERN (project-relative prefix/exact; repeatable).",
)
@click.pass_context
def checkout(
    ctx: click.Context,
    targets: tuple[str, ...],
    checkout_mode: str | None,
    force: bool,
    only_missing: bool,
    exclude: tuple[str, ...],
) -> None:
    """Restore tracked files and stage outputs from cache.

    If no targets specified, restores all tracked files and stage outputs.
    Without --force, existing files with untracked local changes are never
    overwritten -- checkout errors instead. Use --only-missing to only restore
    files that don't exist (and fill missing entries inside existing directories).
    """
    if force and only_missing:
        raise click.ClickException("--force and --only-missing are mutually exclusive")

    # Convert CLI flags to behavior enum
    if force:
        behavior = CheckoutBehavior.FORCE
    elif only_missing:
        behavior = CheckoutBehavior.SKIP_EXISTING
    else:
        behavior = CheckoutBehavior.SAFE

    cli_ctx = cli_helpers.get_cli_context(ctx)
    quiet = cli_ctx["quiet"]

    project_root = project.get_project_root()
    cache_dir = config.get_cache_dir() / "files"

    # Determine checkout modes - CLI flag overrides config (single mode, no fallback)
    checkout_modes = (
        [cache.CheckoutMode(checkout_mode)] if checkout_mode else config.get_checkout_mode_order()
    )

    # Discover tracked files
    tracked_files = track.discover_pvt_files(project_root)

    # Get stage output info from lock files (cached outputs only)
    pipeline = cli_decorators.get_pipeline_from_context()
    stage_outputs = {} if pipeline is None else _get_stage_output_info()

    # Drop excluded paths so they are neither restored nor reported as missing
    matcher = path_utils.make_exclude_matcher(exclude)
    if matcher is not None:
        tracked_files = {
            p: v
            for p, v in tracked_files.items()
            if not matcher(project.to_relative_path(p, project_root))
        }
        stage_outputs = {
            p: v
            for p, v in stage_outputs.items()
            if not matcher(project.to_relative_path(p, project_root))
        }

    state_dir = config.get_state_dir()

    # Run async checkout. SAFE behavior hashes existing files to detect untracked
    # local changes; a read-only StateDB makes unchanged files an O(1) stat and lets
    # the parallel restore threads share lock-free MVCC reads.
    with contextlib.ExitStack() as stack:
        state_db = (
            stack.enter_context(state.StateDB(state_dir, readonly=True))
            if behavior is CheckoutBehavior.SAFE
            else None
        )
        progress = stack.enter_context(cli_helpers.TransferProgress("Restoring", quiet=quiet))
        failures, restored, skipped = asyncio.run(
            _checkout_main_async(
                targets,
                tracked_files,
                stage_outputs,
                cache_dir,
                checkout_modes,
                behavior,
                callback=progress.callback,
                state_dir=state_dir,
                state_db=state_db,
            )
        )

    success = _print_summary(failures, restored, skipped, quiet)
    if not success:
        ctx.exit(1)
