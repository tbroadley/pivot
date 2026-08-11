---
tags: [python, fingerprinting, manifest, determinism, caching]
category: bug
module: fingerprint
symptoms: ["stage reports Code changed on a clean checkout", "fingerprint differs between two processes", "PYTHONHASHSEED changes a lock file", "edit to a helper does not invalidate its stage"]
---

# Manifest Keys Collided Across Modules

## Problem

A stage's manifest is one flat dict holding the transitive closure of every module the
stage reaches, but keys were built from bare symbol names: `const:MARGIN`,
`func:render`, `schema:Params`, `class:Config`. Two modules that define the same name
therefore wrote the same key, and whichever entry was merged last won.

Two consequences, in increasing order of severity:

1. **The recorded value can be the wrong module's.** A stage whose own module sets
   `MARGIN = 0.26` and which calls into a module setting `MARGIN = 0.75` records one of
   the two, so `pivot verify` can report `Code changed` on a checkout nobody edited.
2. **A change can go undetected.** With two `Params` models only one `schema:Params`
   existed; editing the model that lost the key left the manifest unchanged.

Traversal order decided the winner, and part of that order came from iterating a `set`
of global names collected from nested code objects. Set iteration order of strings
depends on `PYTHONHASHSEED`, so the *same* code could fingerprint differently in two
processes — enough that a project can end up pinning `PYTHONHASHSEED=0` in CI to keep
lock files reproducible.

## Solution

Every key but `self:` names a module:

- Keys derived from a definition — `func:`, `class:`, `mod:`, `schema:`, `loader:` —
  use the object's own `__module__` and `__qualname__`. Reaching the same function
  through two different names now collapses onto one key rather than duplicating it.
- Keys for a value with no definition site of its own — `const:`, `partial:`, and
  lambdas — use the module of the function that references them. `0.26` cannot say
  which module wrote it, and a lambda keyed by source position would move whenever a
  line was inserted above it.
- `mod:` uses the module's real `__name__`, not the local import alias, so two modules
  each imported as `base` no longer share keys.

The nested-globals walk is sorted as well, so no traversal order depends on the hash
seed.

## Verifying a Fix Here

Fingerprint a stage that reaches two same-named symbols under several
`PYTHONHASHSEED` values in **separate processes** and compare. In-process repetition
proves nothing: the seed is fixed for the life of the interpreter.

A single-module test proves nothing either — the collision needs a second module, which
is why the regression tests write two modules to a temp dir and import both.

## Key Insight

A key in a merged namespace has to identify the thing, not the name it was reached by.
Anything else is a silent last-write-wins whose outcome depends on walk order — and walk
order is not something a fingerprint should ever depend on.
