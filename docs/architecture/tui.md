# TUI Architecture

The Terminal User Interface provides a real-time view of pipeline execution in watch mode.

## Overview

Pivot's TUI is built with [Textual](https://textual.textualize.io/), an async Python TUI framework. It displays stage status, logs, and execution history while the watch engine monitors for file changes.

## Communication Architecture

```
┌────────────────┐    TuiStatusMessage     ┌──────────────────┐
│    Executor    │ ─────────────────────▶  │  TUI Event Loop  │
│   (workers)    │    TuiLogMessage         │   (Textual)      │
└────────────────┘ ─────────────────────▶  └──────────────────┘
        │                                           │
        │         multiprocessing.Queue             │
        └───────────────────────────────────────────┘
                            │
                   ┌────────┴────────┐
                   │ Background      │
                   │ _read_queue()   │
                   │ thread (0.02s)  │
                   └─────────────────┘
```

### Cross-Process Communication

Workers run in separate processes (via loky), so communication uses a `multiprocessing.Queue`:

1. **Workers** write messages to the queue during execution
2. **Background thread** polls the queue every 20ms
3. **post_message()** delivers messages to Textual's event loop

This pattern is necessary because:

- Workers can't directly call Textual methods (different process)
- Textual's event loop can't block on a multiprocessing queue
- The background thread bridges the two

### Thread-to-Event-Loop Safety

The background thread uses Textual's `post_message()` to safely deliver messages to the event loop:

```python
def _read_queue(self) -> None:
    """Background thread that reads from mp.Queue and posts to Textual."""
    while not self._shutdown.is_set():
        try:
            msg = self._queue.get(timeout=0.02)
            self.post_message(TuiUpdate(msg))
        except queue.Empty:
            continue
```

`post_message()` is thread-safe and schedules the message to be handled in the event loop's next iteration.

## Message Types

| Message | Source | Purpose |
|---------|--------|---------|
| `TuiStatusMessage` | Worker | Stage lifecycle (started, completed, failed, skipped) |
| `TuiLogMessage` | Worker | stdout/stderr lines from stage execution |
| `TuiWatchMessage` | Watch engine | Watch status (waiting, detecting, restarting, error) |
| `TuiReloadMessage` | Watch engine | Stage list changed after hot reload |

### TuiStatusMessage

Sent when a stage transitions state:

```python
TuiStatusMessage(
    stage="train",
    status=StageStatus.COMPLETED,
    duration_ms=1234,
    outputs={"model": "models/model.pkl"}
)
```

### TuiLogMessage

Sent for each line of stage output:

```python
TuiLogMessage(
    stage="train",
    stream="stdout",  # or "stderr"
    line="Epoch 1/10: loss=0.523"
)
```

### TuiWatchMessage

Sent when watch engine state changes:

```python
TuiWatchMessage(
    status=WatchStatus.DETECTING,
    changed_files=["src/model.py"]
)
```

### TuiReloadMessage

Sent after hot-reloading the registry:

```python
TuiReloadMessage(
    stages=["preprocess", "train", "evaluate"],
    removed=["old_stage"],
    added=["new_stage"]
)
```

## Execution History

The TUI maintains execution history for each stage:

```python
# Bounded deque per stage (maxlen=50)
self._history: dict[str, deque[ExecutionRecord]] = defaultdict(
    lambda: deque(maxlen=50)
)
```

Each `ExecutionRecord` captures:

- **Timestamp:** When execution started
- **Duration:** How long it took (or None if still running)
- **Status:** Completed, failed, or skipped
- **Logs:** stdout/stderr output
- **Inputs:** Dependency file snapshots
- **Outputs:** Output file snapshots

This enables "time-travel" viewing of past executions. Select a stage and scroll through its history to see logs and inputs/outputs from previous runs.

## UI Components

```
┌─────────────────────────────────────────────────────────────────────┐
│  pivot run --watch                                            [?]   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Stages:                              │  Logs:                       │
│  ┌────────────────────────────────────┼──────────────────────────────│
│  │ ✓ preprocess    [completed]  0.5s  │  [train] Epoch 1/10         │
│  │ ● train         [running]    ...   │  [train] loss=0.523         │
│  │ ○ evaluate      [pending]          │  [train] Epoch 2/10         │
│  │                                    │  [train] loss=0.412         │
│  │                                    │                              │
│  └────────────────────────────────────┴──────────────────────────────│
│                                                                      │
│  Watching for changes...                                             │
└─────────────────────────────────────────────────────────────────────┘
```

| Component | Description |
|-----------|-------------|
| **Stage List** | Shows all stages with status indicators |
| **Log Panel** | Real-time logs from selected stage |
| **Status Bar** | Watch engine status and help hints |
| **History Selector** | Navigate through past executions |

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `j/k` or arrows | Navigate stage list |
| `Enter` | Select stage for log view |
| `r` | Force re-run selected stage |
| `R` | Force re-run all stages |
| `h` | Toggle history panel |
| `[/]` | Navigate execution history |
| `q` | Quit watch mode |
| `?` | Show help |

## Error Display

When a stage fails, the TUI shows:

1. **Stage status:** Red indicator with "failed"
2. **Error in logs:** Full traceback and error message
3. **Downstream impact:** Pending stages that won't run

```
│ ✓ preprocess    [completed]  0.5s  │  [train] Traceback:           │
│ ✗ train         [failed]     1.2s  │  [train]   File "model.py"    │
│ ○ evaluate      [blocked]          │  [train]     raise ValueError │
```

The watch engine continues monitoring. Fix the error, save the file, and the pipeline automatically re-runs.

## Performance Considerations

- **Queue polling:** 20ms interval balances responsiveness vs CPU usage
- **History limit:** 50 entries per stage prevents memory growth
- **Log buffering:** Large outputs are buffered and truncated in the UI
- **Reactive updates:** Textual only re-renders changed components

## See Also

- [Watch Execution Engine](watch.md) - Watch mode architecture
- [Agent Server](agent-server.md) - JSON-RPC interface
- [Execution Model](execution.md) - Stage execution details
