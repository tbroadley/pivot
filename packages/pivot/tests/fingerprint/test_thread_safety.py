"""Concurrent fingerprinting must be safe.

status computes stage explanations via a ThreadPoolExecutor, so the public fingerprint
entry points can be called from multiple threads at once. The module-level caches they
use are not thread-safe on their own; _fingerprint_lock serializes them. Without it,
concurrent fingerprinting corrupts shared state and raises spurious errors (e.g. an enum
captured as a default arg being misreported as a mutable capture).
"""

import dataclasses
import enum
from concurrent.futures import ThreadPoolExecutor

from pivot import fingerprint


class _Condition(enum.Enum):
    A = "a"
    B = "b"
    C = "c"


@dataclasses.dataclass(frozen=True)
class _Style:
    marker: str


_CONDITIONS = (_Condition.A, _Condition.B)
_STYLES = (("first", _Style("o")), ("second", _Style("s")))
_NUMBERS = (1, 2, 3)
_DEFAULT_CONDITION = _Condition.A


def _helper(x: int) -> int:
    return x + 1


def _stage_enum_tuple() -> tuple[_Condition, ...]:
    return _CONDITIONS


def _stage_frozen_tuple() -> tuple[tuple[str, _Style], ...]:
    return _STYLES


def _stage_enum_default(condition: _Condition = _DEFAULT_CONDITION) -> object:
    return (_NUMBERS, _helper(1), condition)


_STAGE_FUNCS = (_stage_enum_tuple, _stage_frozen_tuple, _stage_enum_default)


def test_concurrent_fingerprinting_matches_single_threaded() -> None:
    baseline = {fn.__name__: fingerprint.get_stage_fingerprint(fn) for fn in _STAGE_FUNCS}

    work = list(_STAGE_FUNCS) * 40
    for _ in range(10):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(fingerprint.get_stage_fingerprint, work))
        for fn, manifest in zip(work, results, strict=True):
            assert manifest == baseline[fn.__name__], (
                "Concurrent fingerprint must equal the single-threaded result"
            )
