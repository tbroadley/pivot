# Code Tour

This guide maps Pivot's architectural concepts to actual file paths, helping you find where to start reading.

## Entry Points

| Command | Entry Point | Description |
|---------|-------------|-------------|
| `pivot run` | `src/pivot/cli/run.py` | Main execution flow |
| `pivot list` | `src/pivot/cli/list.py` | Stage listing |
| `pivot explain` | `src/pivot/explain.py` | Change detection explanation |
| `pivot run --watch` | `src/pivot/watch/engine.py` | Watch mode |

## Core Subsystems

### Pipeline Discovery & Registration

**Key files:**

- `src/pivot/discovery.py` - Auto-discovers `pivot.yaml`, `pipeline.py`
- `src/pivot/yaml_config.py` - Parses `pivot.yaml` into internal structures
- `src/pivot/registry.py` - Global stage registry (`REGISTRY`)
- `src/pivot/stage_def.py` - Stage definition classes (`StageDef`, `StageParams`)

**How it works:**

1. `discovery.discover_and_register()` finds pipeline definition
2. For YAML: `yaml_config.load_pipeline()` parses and registers stages
3. For Python: module is imported, which calls `REGISTRY.register()`

**Start reading:** `src/pivot/discovery.py:discover_and_register()`

### DAG Construction

**Key files:**

- `src/pivot/dag.py` - Builds dependency graph from stages

**How it works:**

1. `build_dag()` takes registered stages
2. Creates nodes for each stage
3. Adds edges based on output→input path relationships
4. Returns topologically sorted execution order

**Start reading:** `src/pivot/dag.py:build_dag()`

### Code Fingerprinting

**Key files:**

- `src/pivot/fingerprint.py` - Main fingerprinting logic
- `src/pivot/ast_utils.py` - AST manipulation helpers

**How it works:**

1. `get_stage_fingerprint()` starts from stage function
2. Inspects closure variables via `inspect.getclosurevars()`
3. Parses AST to find `module.function` patterns
4. Recursively fingerprints all dependencies
5. Normalizes and hashes the combined code

**Start reading:** `src/pivot/fingerprint.py:get_stage_fingerprint()`

### Execution

**Key files:**

- `src/pivot/executor/core.py` - Main executor logic
- `src/pivot/executor/worker.py` - Worker process code
- `src/pivot/executor/commit.py` - Post-execution output handling

**How it works:**

1. `Executor.run()` is the main entry point
2. Uses `loky.get_reusable_executor()` for warm worker pool
3. Workers execute stages via `worker.run_stage()`
4. `commit.commit_outputs()` caches results and updates lock files

**Start reading:** `src/pivot/executor/core.py:Executor.run()`

### Caching & Storage

**Key files:**

- `src/pivot/storage/cache.py` - Content-addressable file cache
- `src/pivot/storage/lock.py` - Per-stage lock files
- `src/pivot/storage/state.py` - LMDB state database
- `src/pivot/storage/restore.py` - Restoring outputs from cache

**How it works:**

1. `CacheStore` hashes and stores file contents by xxhash64
2. `LockFile` records stage fingerprint + output hashes
3. `StateDB` provides fast key-value storage for runtime state
4. `restore_outputs()` retrieves cached files on cache hit

**Start reading:** `src/pivot/storage/cache.py:CacheStore`

### Watch Engine

**Key files:**

- `src/pivot/watch/engine.py` - Main watch loop
- `src/pivot/watch/_watch_utils.py` - Helper utilities

**How it works:**

1. `WatchEngine` starts watcher thread using `watchfiles`
2. Changes are debounced in coordinator loop
3. Code changes trigger worker pool restart
4. Affected stages are re-executed

**Start reading:** `src/pivot/watch/engine.py:WatchEngine`

### TUI

**Key files:**

- `src/pivot/tui/run.py` - Run mode display
- `src/pivot/tui/console.py` - Console rendering
- `src/pivot/tui/agent_server.py` - JSON-RPC server for external control

**How it works:**

1. `RunDisplay` renders stage status during execution
2. Uses `rich` library for terminal output
3. `AgentServer` provides JSON-RPC endpoint for programmatic control

**Start reading:** `src/pivot/tui/run.py:RunDisplay`

### Remote Storage

**Key files:**

- `src/pivot/remote/storage.py` - S3 operations
- `src/pivot/remote/sync.py` - Push/pull logic
- `src/pivot/remote/config.py` - Remote configuration

**How it works:**

1. `RemoteStorage` abstracts S3 operations
2. `push_outputs()` uploads cache files to S3
3. `pull_outputs()` downloads cache files by hash

**Start reading:** `src/pivot/remote/sync.py:push_outputs()`

## Data Flow: `pivot run`

```
CLI (run.py)
    │
    ▼
Discovery (discovery.py)
    │
    ▼
Registry (registry.py) ◄── YAML Parser (yaml_config.py)
    │
    ▼
DAG Builder (dag.py)
    │
    ▼
Executor (executor/core.py)
    │
    ├──► Worker Pool (executor/worker.py)
    │         │
    │         ▼
    │    Stage Function
    │         │
    │         ▼
    │    Commit Outputs (executor/commit.py)
    │         │
    │         ▼
    │    Cache (storage/cache.py)
    │
    ▼
Lock File Update (storage/lock.py)
```

## Key Design Patterns

### Module-Level Functions

All stage-related functions must be module-level for pickling. See `src/pivot/fingerprint.py` for how we detect and handle this.

### Content-Addressable Storage

Files are stored by hash, enabling deduplication. See `src/pivot/storage/cache.py:CacheStore.store()`.

### Per-Stage Lock Files

Each stage has its own lock file for O(n) updates instead of O(n²). See `src/pivot/storage/lock.py`.

### Reusable Worker Pool

Workers stay warm across executions. See `src/pivot/executor/core.py` use of `loky.get_reusable_executor()`.

## Testing

Test structure mirrors source:

| Source | Tests |
|--------|-------|
| `src/pivot/fingerprint.py` | `tests/fingerprint/` |
| `src/pivot/executor/` | `tests/unit/test_executor.py` |
| `src/pivot/cli/` | `tests/integration/test_cli_*.py` |

See `tests/CLAUDE.md` for testing guidelines.

## Adding Features

### New CLI Command

1. Create `src/pivot/cli/mycommand.py`
2. Use `@cli_decorators.pivot_command()` decorator
3. Add to `src/pivot/cli/__init__.py`
4. Add tests in `tests/integration/test_cli_mycommand.py`

See [CLI Development](../contributing/cli.md)

### New Loader Type

1. Add to `src/pivot/loaders.py`
2. Extend `Loader[T]` base class
3. Implement `load()` and `save()`
4. Add tests

See [Adding Loaders](../contributing/loaders.md)

### New Output Type

1. Add to `src/pivot/outputs.py`
2. Define handling in `src/pivot/executor/commit.py`
3. Add tests

## See Also

- [Architecture Overview](overview.md) - High-level design
- [Fingerprinting](fingerprinting.md) - How code tracking works
- [Execution Model](execution.md) - Parallel execution details
