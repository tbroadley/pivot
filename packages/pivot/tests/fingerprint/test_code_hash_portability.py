# pyright: reportUnusedFunction=false, reportUnknownParameterType=false, reportMissingParameterType=false, reportUnknownArgumentType=false
"""Bytecode hashes must not depend on where the source file lives.

Functions hashed via their code object (wrapped functions, functions without
retrievable source) used to be hashed with a bare `marshal.dumps(func.__code__)`.
A marshalled code object embeds `co_filename` — an absolute path — so the same
code produced a different hash in every checkout and under every interpreter
install, making lock files valid only on the machine that wrote them.

These tests simulate the second checkout in-process by rebuilding the code object
with a different filename, which is exactly the difference that a real second
checkout introduces.
"""

import contextlib
import functools
import types
from collections.abc import Callable
from typing import Any

from pivot import fingerprint


def _relocate(func: Callable[..., Any], filename: str) -> Callable[..., Any]:
    """Return a copy of `func` as if its source file lived at `filename`.

    Nested code objects (comprehensions, inner functions) carry their own
    `co_filename`, so they are relocated too — same as a real second checkout.
    """

    def relocate_code(code: types.CodeType) -> types.CodeType:
        consts = tuple(
            relocate_code(const) if isinstance(const, types.CodeType) else const
            for const in code.co_consts
        )
        return code.replace(co_filename=filename, co_consts=consts)

    relocated = types.FunctionType(
        relocate_code(func.__code__),
        func.__globals__,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )
    relocated.__qualname__ = func.__qualname__
    wrapped = getattr(func, "__wrapped__", None)
    if wrapped is not None:
        relocated.__wrapped__ = wrapped  # pyright: ignore[reportFunctionMemberAccess]
    return relocated


def _decorator(func):  # type: ignore[no-untyped-def]
    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        return func(*args, **kwargs)

    return wrapper


@_decorator
def _wrapped_stage() -> int:
    return 42


@contextlib.contextmanager
def _managed():
    yield 1


def _stage_with_nested_function(values: list[int]) -> list[int]:
    def double(value: int) -> int:
        return value * 2

    return list(map(double, values))


def test_wrapped_function_hash_independent_of_source_path():
    """A wraps-decorated function hashes the same from two checkout paths."""
    from_checkout_a = fingerprint._compute_function_hash(_wrapped_stage)
    from_checkout_b = fingerprint._compute_function_hash(
        _relocate(_wrapped_stage, "/other/checkout/pipeline.py")
    )

    assert from_checkout_a == from_checkout_b, (
        "Wrapped function hash must not depend on the absolute path of its source file"
    )


def test_contextmanager_hash_independent_of_interpreter_path():
    """`@contextlib.contextmanager` applies `functools.wraps` itself.

    The wrapper's code object comes from contextlib, so its `co_filename` is the
    interpreter install path — different on every machine.
    """
    from_install_a = fingerprint._compute_function_hash(_managed)
    from_install_b = fingerprint._compute_function_hash(
        _relocate(_managed, "/other/python/lib/python3.13/contextlib.py")
    )

    assert from_install_a == from_install_b, (
        "contextmanager hash must not depend on the interpreter install path"
    )


def test_nested_code_objects_are_relocated_too():
    """Inner functions are nested code constants carrying their own `co_filename`."""
    code = _stage_with_nested_function.__code__
    nested = [const for const in code.co_consts if isinstance(const, types.CodeType)]
    assert nested, "expected the inner function to compile to a nested code object"

    stripped = fingerprint._strip_code_filenames(code)
    stripped_nested = [const for const in stripped.co_consts if isinstance(const, types.CodeType)]

    assert stripped.co_filename == fingerprint._PORTABLE_CO_FILENAME
    assert [const.co_filename for const in stripped_nested] == [
        fingerprint._PORTABLE_CO_FILENAME
    ] * len(stripped_nested)


def test_hash_ignores_filename_of_nested_code_only():
    """A difference confined to a nested code object's filename is still ignored."""
    code = _stage_with_nested_function.__code__
    relocated_consts = tuple(
        const.replace(co_filename="/other/checkout/pipeline.py")
        if isinstance(const, types.CodeType)
        else const
        for const in code.co_consts
    )

    assert fingerprint.hash_code_object(code) == fingerprint.hash_code_object(
        code.replace(co_consts=relocated_consts)
    )


def test_stripping_filenames_preserves_everything_else():
    """Only filenames are blanked — bytecode, constants and names are untouched."""
    code = _stage_with_nested_function.__code__
    stripped = fingerprint._strip_code_filenames(code)

    assert stripped.co_code == code.co_code
    assert stripped.co_names == code.co_names
    assert stripped.co_varnames == code.co_varnames
    assert stripped.co_qualname == code.co_qualname
    assert stripped.co_firstlineno == code.co_firstlineno


def test_code_hash_still_detects_logic_changes():
    """Path independence must not cost sensitivity to real changes."""

    def add_one(x: int) -> int:
        return x + 1

    def add_many(x: int) -> int:
        return x + 999

    assert fingerprint.hash_code_object(add_one.__code__) != fingerprint.hash_code_object(
        add_many.__code__
    ), "constants must still be part of the hash"


def test_code_hash_detects_decorator_change_across_paths():
    """Relocation must not mask a genuine difference in wrapper logic."""

    def loud_decorator(func):  # type: ignore[no-untyped-def]
        @functools.wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
            print("LOUD")
            return func(*args, **kwargs)

        return wrapper

    @loud_decorator
    def stage() -> int:
        return 42

    quiet = fingerprint._compute_function_hash(_wrapped_stage)
    loud = fingerprint._compute_function_hash(_relocate(stage, "/other/checkout/pipeline.py"))

    assert quiet != loud, "different decorator logic must still produce different hashes"
