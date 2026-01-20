from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, override

import rich.markup
import textual.app
import textual.containers
import textual.css.query
import textual.widgets

from pivot.tui.diff_panels import InputDiffPanel, OutputDiffPanel
from pivot.tui.widgets.logs import StageLogPanel
from pivot.tui.widgets.status import format_elapsed, get_status_icon, get_status_label

if TYPE_CHECKING:
    from pivot.tui.types import ExecutionHistoryEntry, StageInfo

_logger = logging.getLogger(__name__)


class DetailPanel(textual.widgets.Static):
    """Panel showing details of selected stage."""

    _stage: StageInfo | None

    def __init__(self, *, id: str | None = None, classes: str | None = None) -> None:
        super().__init__(id=id, classes=classes)
        self._stage = None

    def set_stage(self, stage: StageInfo | None) -> None:  # pragma: no cover
        self._stage = stage
        self._update_display()

    def _update_display(self) -> None:  # pragma: no cover
        if self._stage is None:
            self.update("[dim]No stage selected[/]")
            return

        label, style = get_status_label(self._stage.status)
        elapsed_str = format_elapsed(self._stage.elapsed)
        if elapsed_str:
            elapsed_str = f" {elapsed_str} elapsed"

        lines = [
            f"[bold]Stage:[/] {rich.markup.escape(self._stage.name)}",
            f"[bold]Status:[/] [{style}]{label}[/]{elapsed_str}",
        ]
        if self._stage.reason:
            lines.append(f"[bold]Reason:[/] {rich.markup.escape(self._stage.reason)}")

        self.update("\n".join(lines))


class TabbedDetailPanel(textual.containers.Vertical):
    """Tabbed panel showing stage details with Logs, Input, Output tabs."""

    _stage: StageInfo | None
    _history_index: int | None  # None = live view, else index into history deque
    _history_total: int

    def __init__(self, *, id: str | None = None, classes: str | None = None) -> None:
        super().__init__(id=id, classes=classes)
        self._stage = None
        self._history_index = None
        self._history_total = 0

    @override
    def compose(self) -> textual.app.ComposeResult:  # pragma: no cover
        yield textual.widgets.Static(id="detail-header")
        with textual.widgets.TabbedContent(id="detail-tabs"):
            with textual.widgets.TabPane("Logs", id="tab-logs"):
                yield StageLogPanel(id="stage-logs")
            with textual.widgets.TabPane("Input", id="tab-input"):
                yield InputDiffPanel(id="input-panel")
            with textual.widgets.TabPane("Output", id="tab-output"):
                yield OutputDiffPanel(id="output-panel")

    def set_stage(self, stage: StageInfo | None) -> None:  # pragma: no cover
        """Update the displayed stage."""
        self._stage = stage
        self._history_index = None  # Reset to live view
        self._history_total = len(stage.history) if stage else 0
        stage_name = stage.name if stage else None

        self._update_header()

        # Update log panel (takes StageInfo)
        try:
            self.query_one("#stage-logs", StageLogPanel).set_stage(stage)
        except textual.css.query.NoMatches:
            _logger.debug("stage-logs not found during set_stage")

        # Update Input panel - use live snapshot if available
        try:
            input_panel = self.query_one("#input-panel", InputDiffPanel)
            if stage and stage.live_input_snapshot:
                input_panel.set_from_snapshot(stage.live_input_snapshot)
            else:
                input_panel.set_stage(stage_name)
        except textual.css.query.NoMatches:
            _logger.debug("input-panel not found during set_stage")

        # Update Output panel - use live snapshot if available, pass status for empty state
        try:
            output_panel = self.query_one("#output-panel", OutputDiffPanel)
            status = stage.status if stage else None
            if stage and stage.live_output_snapshot:
                output_panel.set_from_snapshot(
                    stage.name, stage.live_output_snapshot, status=status
                )
            else:
                output_panel.set_stage(stage_name, status=status)
        except textual.css.query.NoMatches:
            _logger.debug("output-panel not found during set_stage")

    def set_history_view(
        self, index: int | None, total: int, entry: ExecutionHistoryEntry | None
    ) -> None:  # pragma: no cover
        """Set the history view state. index=None means live view."""
        self._history_index = index
        self._history_total = total
        self._update_header()

        if entry is not None:
            # Update logs panel with historical logs
            try:
                log_panel = self.query_one("#stage-logs", StageLogPanel)
                log_panel.set_from_history(entry.logs)
            except textual.css.query.NoMatches:
                _logger.debug("stage-logs not found during set_history_view")

    def _update_header(self) -> None:  # pragma: no cover
        """Update the header with execution indicator."""
        try:
            header = self.query_one("#detail-header", textual.widgets.Static)
        except textual.css.query.NoMatches:
            return

        if self._stage is None:
            header.update("")
            return

        # Build header components
        parts = list[str]()

        # Stage name
        parts.append(f"[bold]{rich.markup.escape(self._stage.name)}[/]")

        # Spacer
        parts.append("  ")

        # History navigation indicator
        total = self._history_total
        if self._history_index is None:
            # Live view
            current = total + 1  # Live is "after" all history entries
            # Show left arrow if history exists
            left_arrow = "← " if total > 0 else ""
            mode_indicator = "[green]● LIVE[/]"
            parts.append(f"{left_arrow}[{current}/{current}] {mode_indicator}")
        else:
            # Historical view
            current = self._history_index + 1  # 1-based display
            left_arrow = "← " if self._history_index > 0 else ""
            right_arrow = " →"  # Always show - can navigate to live view

            # Get entry for timestamp/duration
            entry = self._get_current_history_entry()
            if entry:
                ts_str = time.strftime("%H:%M:%S", time.localtime(entry.timestamp))
                dur_str = f"{entry.duration:.1f}s" if entry.duration is not None else "0.0s"
                status_icon = get_status_icon(entry.status)
                mode_indicator = f"[yellow]◷ {ts_str} ({dur_str})[/] {status_icon}"
            else:
                mode_indicator = "[yellow]◷ (unknown)[/]"

            parts.append(f"{left_arrow}[{current}/{total + 1}] {mode_indicator}{right_arrow}")

        header.update("".join(parts))

    def _get_current_history_entry(self) -> ExecutionHistoryEntry | None:
        """Get the currently viewed history entry."""
        if self._stage is None or self._history_index is None:
            return None
        if 0 <= self._history_index < len(self._stage.history):
            return self._stage.history[self._history_index]
        return None

    def refresh_header(self) -> None:  # pragma: no cover
        """Refresh the header display (call when history changes)."""
        if self._stage:
            self._history_total = len(self._stage.history)
        self._update_header()
