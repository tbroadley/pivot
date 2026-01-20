# Agent Server

The Agent Server provides a JSON-RPC interface for external tools to interact with Pivot's watch mode.

## Overview

When running in watch mode with the TUI, Pivot starts an Agent Server that exposes pipeline operations over a Unix socket. This enables IDE integrations, external automation tools, and custom interfaces to control pipeline execution.

## Protocol

**Transport:** Unix domain socket at `.pivot/agent.sock`

**Protocol:** JSON-RPC 2.0

**Security:** Socket permissions `0o600` (owner-only read/write)

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "run",
  "params": {"stages": ["train"], "force": false},
  "id": 1
}
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "result": {"run_id": "abc123", "queued": true},
  "id": 1
}
```

## Available Methods

| Method | Params | Description |
|--------|--------|-------------|
| `run` | `stages?`, `force?` | Queue execution request |
| `status` | `run_id?` | Get current execution state |
| `stages` | - | List all registered stages |
| `cancel` | - | Request cancellation (not yet supported) |

### run

Queue stages for execution.

**Parameters:**

- `stages` (optional): List of stage names to run. If omitted, runs all stages.
- `force` (optional): If true, ignore cache and force re-execution.

**Returns:**

- `run_id`: Unique identifier for this execution
- `queued`: Whether the request was queued successfully

**Example:**

```json
// Request
{"jsonrpc": "2.0", "method": "run", "params": {"stages": ["preprocess", "train"]}, "id": 1}

// Response
{"jsonrpc": "2.0", "result": {"run_id": "run-20240115-143052", "queued": true}, "id": 1}
```

### status

Get the current state of the pipeline or a specific run.

**Parameters:**

- `run_id` (optional): Specific run to query. If omitted, returns current state.

**Returns:**

- `state`: Current state (IDLE, WATCHING, RUNNING, COMPLETED, FAILED)
- `run_id`: Current or specified run ID
- `stages`: Map of stage names to their status

**Example:**

```json
// Request
{"jsonrpc": "2.0", "method": "status", "id": 2}

// Response
{
  "jsonrpc": "2.0",
  "result": {
    "state": "RUNNING",
    "run_id": "run-20240115-143052",
    "stages": {
      "preprocess": {"status": "completed", "duration_ms": 1234},
      "train": {"status": "running", "started_at": "2024-01-15T14:30:55Z"}
    }
  },
  "id": 2
}
```

### stages

List all registered stages and their metadata.

**Parameters:** None

**Returns:**

- List of stage objects with name, deps, outs, and params

**Example:**

```json
// Request
{"jsonrpc": "2.0", "method": "stages", "id": 3}

// Response
{
  "jsonrpc": "2.0",
  "result": [
    {"name": "preprocess", "deps": ["data/raw.csv"], "outs": ["data/clean.csv"]},
    {"name": "train", "deps": ["data/clean.csv"], "outs": ["models/model.pkl"]}
  ],
  "id": 3
}
```

### cancel

Request cancellation of current execution.

**Parameters:** None

**Returns:**

- `cancelled`: Always `false` (not yet implemented)

**Note:** Cancellation is not yet supported. The method exists for API completeness and will return `cancelled: false`. See Limitations below.

## State Machine

The Agent Server tracks pipeline state through these transitions:

```
IDLE → WATCHING → RUNNING → COMPLETED/FAILED → WATCHING
                     ↑                              │
                     └──────────────────────────────┘
```

| State | Description |
|-------|-------------|
| `IDLE` | Server started, not yet watching |
| `WATCHING` | Monitoring for file changes |
| `RUNNING` | Executing stages |
| `COMPLETED` | Last run succeeded |
| `FAILED` | Last run had failures |

After `COMPLETED` or `FAILED`, the state returns to `WATCHING` to await further changes or explicit run requests.

## Thread Safety

The Agent Server uses `_agent_lock` to protect atomic operations:

```python
with self._agent_lock:
    if self._state == AgentState.RUNNING:
        return {"queued": False, "reason": "already running"}
    self._state = AgentState.RUNNING
    self._pending_run = run_request
```

This ensures that concurrent RPC calls don't race on state checks and updates.

## Client Example

Using Python with the `jsonrpcclient` library:

```python
import socket
import json

def call_agent(method: str, params: dict | None = None) -> dict:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(".pivot/agent.sock")

    request = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": 1
    }

    sock.sendall(json.dumps(request).encode() + b"\n")
    response = sock.recv(4096).decode()
    sock.close()

    return json.loads(response)

# Trigger a run
result = call_agent("run", {"stages": ["train"], "force": True})
print(f"Run queued: {result['result']['queued']}")

# Check status
status = call_agent("status")
print(f"State: {status['result']['state']}")
```

## Limitations

- **Cancellation not supported:** The `cancel` method returns `cancelled: false`. The current executor doesn't support mid-execution cancellation. Workaround: wait for completion or restart the watch process.

- **Single client:** The server handles one request at a time. Concurrent requests are serialized.

- **Local only:** Unix sockets are local-machine only. For remote access, use SSH tunneling or a proxy.

## See Also

- [Watch Execution Engine](watch.md) - Watch mode architecture
- [TUI Architecture](tui.md) - Terminal UI communication
