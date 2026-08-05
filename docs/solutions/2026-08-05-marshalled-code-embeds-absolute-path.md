---
tags: [python, fingerprinting, marshal, bytecode, caching]
category: bug
module: fingerprint
symptoms: ["lock files only valid on one machine", "same commit fingerprints differently in each checkout", "decorated stage re-runs after moving the repo", "contextmanager stage re-runs on a different machine"]
---

# Marshalled Code Objects Embed an Absolute Path

## Problem

`fingerprint._compute_function_hash()` hashes a function's code object instead of
its source in two cases: when the function carries `__wrapped__` (because
`inspect.getsource()` follows the `__wrapped__` chain and would return the
*original* function's source, hiding the decorator entirely), and when source is
unavailable at all.

That hash used to be `xxhash.xxh64(marshal.dumps(func.__code__))`. A marshalled
code object contains `co_filename`, which is the absolute path of the file the
code was compiled from:

```python
>>> marshal.dumps(func.__code__) == marshal.dumps(
...     func.__code__.replace(co_filename="/somewhere/else.py")
... )
False
```

So any stage entry point decorated with `functools.wraps` fingerprinted
differently in every checkout: three working copies of the same commit produced
three hashes, and a lock file was only valid on the machine that wrote it.

`@contextlib.contextmanager` hits the same bug one level down. It applies
`functools.wraps` itself, and the wrapper's code object comes from `contextlib`
inside the interpreter installation — so the embedded path is the interpreter
install path, and the hash changes between two installs of the *same* Python
version on the same machine.

## Solution

Strip the filenames before marshalling. `fingerprint.hash_code_object()` replaces
`co_filename` with a fixed placeholder on the code object and, recursively, on
every nested code object in `co_consts` (inner functions and generators carry
their own copy):

```python
def _strip_code_filenames(code: types.CodeType) -> types.CodeType:
    consts = tuple(
        _strip_code_filenames(const) if isinstance(const, types.CodeType) else const
        for const in code.co_consts
    )
    return code.replace(co_filename=_PORTABLE_CO_FILENAME, co_consts=consts)
```

Everything else — bytecode, constants, names, qualnames, line numbers — is left
alone, so the hash keeps detecting real changes: `x + 1` and `x + 999` still
differ, and changing a decorator's body still invalidates the stage.

## Verifying a Fix Here

Do **not** compare `marshal.dumps(fn.__code__)` between two processes to check
this: those bytes are path-dependent whether or not the fix is applied, and a
check inside a single tree passes even with the bug present.

The reliable check is two checkouts of the same commit with *separate*
interpreter installations (e.g. different `UV_PYTHON_INSTALL_DIR`), comparing
`_compute_function_hash(fn)` for a wraps-decorated function defined inside each
tree. Separate interpreter installs are what exposes the contextmanager variant.

In-process, the same divergence can be simulated by rebuilding the code object
with a different `co_filename` via `code.replace()` — that is what
`packages/pivot/tests/fingerprint/test_code_hash_portability.py` does.

## Key Insight

Anything reachable from a hash input must be checked for absolute paths, not just
for mutability or ordering. Code objects are the easy one to miss because
`co_filename` is invisible in the bytecode listing and only shows up once the
object is serialized.
