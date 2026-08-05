from __future__ import annotations

import enum
import json
import shutil
import sys
import time
from typing import TYPE_CHECKING, Any, cast, override

import click
from tqdm.asyncio import tqdm as async_tqdm

if TYPE_CHECKING:
    from networkx import DiGraph

    from pivot.cli import CliContext
    from pivot.pipeline.pipeline import Pipeline
    from pivot.registry import RegistryStageInfo, StageRegistry

from pivot import exceptions
from pivot.cli import decorators as cli_decorators

# Captured at import so byte formatting survives tests that monkeypatch ``async_tqdm``.
_format_sizeof = async_tqdm.format_sizeof

# Minimum seconds between byte-postfix repaints (a single large file's file-count bar
# only advances at start/finish, so set_bytes must repaint itself to look live).
_BYTES_REFRESH_INTERVAL = 0.1

# The filename in a transfer bar's prefix is padded/truncated to a fixed width so the
# bar itself doesn't jump left and right as files of different name lengths stream by.
_FILENAME_MIN_WIDTH = 12
_FILENAME_MAX_WIDTH = 60
# Rough width of everything after the filename on a transfer line: the percentage, the
# bar, the count/timing block, and the byte/rate postfix, e.g.
# "  52%|███| 1088.17/2110 [00:51<01:00, 16.96file/s, 983MB (19.6MB/s)]".
_TRANSFER_BAR_RESERVED = 58


def _fit_filename(filename: str, width: int) -> str:
    """Pad or truncate ``filename`` to exactly ``width`` columns.

    Keeping the prefix a constant width stops the progress bar from jumping. Long
    paths keep their tail (the actual file name); the head is elided with a leading
    ellipsis so the most informative part stays visible.
    """
    if len(filename) <= width:
        return filename.ljust(width)
    return "…" + filename[-(width - 1) :]


def _filename_field_width(columns: int, action: str) -> int:
    """Width to allot the filename, clamped so the bar always has room to render."""
    available = columns - len(action) - _TRANSFER_BAR_RESERVED
    return max(_FILENAME_MIN_WIDTH, min(_FILENAME_MAX_WIDTH, available))


class NoPipelineError(exceptions.PivotError):
    """Raised when no Pipeline is available in context."""

    @override
    def format_user_message(self) -> str:
        return (
            "No pipeline definition found.\n"
            "\n"
            "This command requires a pipeline to be defined in one of:\n"
            "  - pivot.yaml (or pivot.yml)\n"
            "  - pipeline.py"
        )


def _get_pipeline() -> Pipeline:
    """Get Pipeline from context, raising NoPipelineError if not found."""
    pipeline = cli_decorators.get_pipeline_from_context()
    if pipeline is None:
        raise NoPipelineError()
    return pipeline


def get_registry() -> StageRegistry:
    """Get StageRegistry from Pipeline in context."""
    return _get_pipeline()._registry  # pyright: ignore[reportPrivateUsage]


def list_stages() -> list[str]:
    """List stage names from Pipeline in context."""
    return _get_pipeline().list_stages()


def get_stage(name: str) -> RegistryStageInfo:
    """Get stage info from Pipeline in context."""
    return _get_pipeline().get(name)


def resolve_external_dependencies() -> None:
    """Resolve external dependencies on the Pipeline in context."""
    _get_pipeline().resolve_external_dependencies()


def get_all_stages() -> dict[str, RegistryStageInfo]:
    """Get all stages as a dict from Pipeline in context."""
    pipeline = _get_pipeline()
    return {name: pipeline.get(name) for name in pipeline.list_stages()}


def build_dag(validate: bool = True) -> DiGraph[str]:
    """Build DAG from Pipeline in context."""
    return _get_pipeline().build_dag(validate=validate)


def _json_default(obj: Any) -> Any:  # noqa: ANN401 - json.dumps default requires Any
    """Handle non-standard JSON types in JSONL output."""
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, set):
        # Convert to sorted list for JSON serialization (deterministic output)
        # Cast needed because isinstance(obj, set) doesn't narrow the element type
        return sorted(cast("set[Any]", obj), key=str)
    # Let json.dumps raise TypeError for truly unserializable types
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def emit_jsonl(event: object) -> None:
    """Emit a single JSONL event to stdout with flush for streaming.

    Handles StrEnum values automatically. Sets are converted to sorted lists.
    Other non-serializable types will raise TypeError.
    """
    print(json.dumps(event, default=_json_default), flush=True)


def get_cli_context(ctx: click.Context) -> CliContext:
    """Get CLI context with defaults if not set."""
    if ctx.obj:
        return ctx.obj
    # Return dict matching CliContext structure to avoid circular import
    return {"verbose": False, "quiet": False}


def stages_to_list(stages: tuple[str, ...]) -> list[str] | None:
    """Convert Click's stage tuple to list or None if empty."""
    return list(stages) if stages else None


def validate_stages_exist(stages: list[str] | None) -> None:
    """Validate that specified stages exist in the registry."""
    if not stages:
        return
    registered = set(list_stages())
    unknown = [s for s in stages if s not in registered]
    if unknown:
        raise exceptions.StageNotFoundError(unknown, available_stages=list(registered))


class TransferProgress:
    """Context manager for transfer progress bars."""

    _action: str
    _bar: async_tqdm[Any] | None
    _show: bool
    _bytes_start: float | None
    _bytes_last_refresh: float

    def __init__(self, action: str, *, quiet: bool = False) -> None:
        self._action = action
        self._bar = None
        self._show = sys.stderr.isatty() and not quiet
        self._bytes_start = None
        self._bytes_last_refresh = 0.0

    def __enter__(self) -> TransferProgress:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        if self._bar is not None:
            self._bar.close()

    def callback(self, completed: float, total: int, filename: str) -> None:
        if not self._show:
            return
        if self._bar is None:
            self._bar = async_tqdm(
                total=total,
                file=sys.stderr,
                dynamic_ncols=True,
                leave=False,
                unit="file",
            )
        width = _filename_field_width(shutil.get_terminal_size().columns, self._action)
        self._bar.desc = f"{self._action} {_fit_filename(filename, width)}"
        # Round so a fractional count renders as e.g. "0.50/1" rather than the raw
        # float; the percentage column carries the precise progress anyway.
        self._bar.update(round(completed, 2) - self._bar.n)

    def set_bytes(self, bytes_done: int) -> None:
        """Show cumulative bytes transferred and average rate as the bar postfix."""
        if not self._show or self._bar is None:
            return
        now = time.monotonic()
        if self._bytes_start is None:
            self._bytes_start = now
        size = _format_sizeof(bytes_done)
        elapsed = now - self._bytes_start
        if elapsed > 0:
            rate = _format_sizeof(bytes_done / elapsed)
            self._bar.set_postfix_str(f"{size}B ({rate}B/s)", refresh=False)
        else:
            self._bar.set_postfix_str(f"{size}B", refresh=False)
        # Repaint on a throttled interval: a single large file only triggers the bar's
        # own redraw at start/finish, so without this the postfix would never climb.
        if now - self._bytes_last_refresh >= _BYTES_REFRESH_INTERVAL:
            self._bytes_last_refresh = now
            self._bar.refresh()


def print_transfer_errors(errors: list[str], max_shown: int = 5) -> None:
    """Print transfer errors with truncation for long lists."""
    if not errors:
        return
    for err in errors[:max_shown]:
        click.echo(f"  Error: {err}", err=True)
    if len(errors) > max_shown:
        click.echo(f"  ... and {len(errors) - max_shown} more errors", err=True)
