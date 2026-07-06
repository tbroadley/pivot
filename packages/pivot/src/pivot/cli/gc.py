from __future__ import annotations

import click
from tqdm.asyncio import tqdm as async_tqdm

from pivot import config
from pivot import gc as gc_mod
from pivot.cli import decorators as cli_decorators
from pivot.cli import helpers as cli_helpers
from pivot.remote import sync as transfer
from pivot.storage import cache as cache_mod
from pivot.storage import state

# Captured at import so byte formatting survives tests that monkeypatch async_tqdm.
_format_sizeof = async_tqdm.format_sizeof


@cli_decorators.pivot_command(auto_discover=False)
@click.option(
    "--workspace",
    "workspace_only",
    is_flag=True,
    help="Scope to the current checkout instead of all worktrees + local branches.",
)
@click.option("--dry-run", "-n", is_flag=True, help="Show what would be removed without deleting.")
@click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.option(
    "-r",
    "--remote",
    "remote_name",
    help="Remote to verify against (uses default if not specified).",
)
@click.option(
    "-j",
    "--jobs",
    type=click.IntRange(min=1),
    default=None,
    help="Parallel jobs for remote existence checks.",
)
@click.pass_context
def gc(
    ctx: click.Context,
    workspace_only: bool,
    dry_run: bool,
    yes: bool,
    remote_name: str | None,
    jobs: int | None,
) -> None:
    """Remove local cache blobs that no live reference points to.

    The referenced ("live") set is every cached stage output and dependency in
    the lock files, plus every .pvt-tracked file. By default it spans ALL linked
    worktrees and local branches of this repo, so collecting from one worktree
    never deletes another's data. Pass --workspace to scope to the current
    checkout only.

    A blob is deleted only if it also exists on the remote, so pushed data can
    always be re-fetched and local-only (unpushed) blobs are never lost. This
    requires a configured remote.
    """
    cli_ctx = cli_helpers.get_cli_context(ctx)
    quiet = cli_ctx["quiet"]

    cache_dir = config.get_cache_dir()
    state_dir = config.get_state_dir()

    scope = gc_mod.GcScope.WORKSPACE if workspace_only else gc_mod.GcScope.ALL
    referenced = gc_mod.collect_referenced_hashes(scope)
    local = transfer.get_local_cache_hashes(cache_dir)
    candidates = local - referenced

    if not candidates:
        if not quiet:
            click.echo(f"Nothing to collect ({len(local)} blob(s), all referenced).")
        return

    jobs = jobs if jobs is not None else config.get_remote_jobs()
    s3_remote, resolved_name = transfer.create_remote_from_name(remote_name)
    with state.StateDB(state_dir) as state_db:
        removable, local_only = transfer.partition_local_by_remote(
            candidates, s3_remote, state_db, resolved_name, jobs
        )

    if local_only and not quiet:
        click.echo(
            f"Keeping {len(local_only)} unreferenced blob(s) not on remote '{resolved_name}'"
            + " (run 'pivot push' to back them up first)."
        )

    if not removable:
        if not quiet:
            click.echo("Nothing to collect (no unreferenced blobs are backed up on the remote).")
        return

    freed = cache_mod.sum_blob_sizes(cache_dir, removable)
    if dry_run:
        if not quiet:
            click.echo(f"Would remove {len(removable)} blob(s), freeing {_format_sizeof(freed)}B.")
        return

    if not yes:
        click.confirm(
            f"Remove {len(removable)} unreferenced blob(s) and free {_format_sizeof(freed)}B?",
            abort=True,
        )

    removed = cache_mod.remove_cache_blobs(cache_dir, removable)
    if not quiet:
        click.echo(f"Removed {removed} blob(s), freed {_format_sizeof(freed)}B.")
