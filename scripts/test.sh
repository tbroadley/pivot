#!/usr/bin/env bash
# Run the two test suites as separate pytest processes.
#
# They cannot share a single invocation: both packages have a tests/ directory
# containing a conftest.py and a helpers.py. pivot's stage discovery only works
# under pytest's "prepend" import mode, which derives module names from the
# directory layout — so a combined run collides on the `tests.conftest` module
# name, and a single shared sys.path makes a bare `import helpers` ambiguous
# between the two packages. Separate processes give each suite its own sys.path,
# sidestepping both problems.
#
# Extra arguments are forwarded to both runs, e.g.:
#   scripts/test.sh -k rpc
#   scripts/test.sh -x -m "not slow"
set -euo pipefail

uv run pytest packages/pivot/tests -n auto "$@"
uv run pytest packages/pivot-tui/tests -n auto "$@"
