import dataclasses
import enum
import logging
from collections.abc import Callable
from typing import ClassVar
from unittest import mock

import pydantic
import pytest

from pivot import exceptions, fingerprint
from pivot.config import models


@dataclasses.dataclass
class MutableConfig:
    value: int


@dataclasses.dataclass(frozen=True)
class FrozenConfig:
    value: int


class MutableModel(pydantic.BaseModel):
    value: int


class FrozenModel(pydantic.BaseModel):
    model_config: ClassVar[pydantic.ConfigDict] = pydantic.ConfigDict(frozen=True)
    value: int


MUTABLE_DICT = {"a": 1}
MUTABLE_LIST = [1, 2]
MUTABLE_SET = {1, 2}
MUTABLE_DATACLASS = MutableConfig(value=1)
MUTABLE_PYDANTIC = MutableModel(value=1)
FROZEN_DATACLASS = FrozenConfig(value=1)
FROZEN_PYDANTIC = FrozenModel(value=1)
IMMUTABLE_TUPLE = (1, 2)
IMMUTABLE_FROZENSET = frozenset({1, 2})
PRIMITIVE_INT = 42


class Basis(enum.Enum):
    FRONTIER = "frontier"
    HEAD = "head"


ENUM_MEMBER = Basis.FRONTIER


class CallableBasis(enum.Enum):
    A = 1
    B = 2

    def __call__(self) -> int:
        return self.value


CALLABLE_ENUM_MEMBER = CallableBasis.A


class ComplexValueBasis(enum.Enum):
    A = object()


COMPLEX_ENUM_MEMBER = ComplexValueBasis.A


def _callable_helper() -> int:
    return 7


def _stage_uses_mutable_dict() -> int:
    return MUTABLE_DICT["a"]


def _stage_uses_mutable_list() -> int:
    return len(MUTABLE_LIST)


def _stage_uses_mutable_set() -> int:
    return len(MUTABLE_SET)


def _stage_uses_mutable_dataclass() -> int:
    return MUTABLE_DATACLASS.value


def _stage_uses_mutable_pydantic() -> int:
    return MUTABLE_PYDANTIC.value


def _stage_uses_frozen_dataclass() -> int:
    return FROZEN_DATACLASS.value


def _stage_uses_frozen_pydantic() -> int:
    return FROZEN_PYDANTIC.value


def _stage_uses_tuple() -> int:
    return len(IMMUTABLE_TUPLE)


def _stage_uses_frozenset() -> int:
    return len(IMMUTABLE_FROZENSET)


def _stage_uses_primitive() -> int:
    return PRIMITIVE_INT


def _stage_uses_enum_member() -> str:
    return ENUM_MEMBER.value


def _stage_uses_callable_enum_member() -> int:
    return CALLABLE_ENUM_MEMBER()


def _stage_uses_complex_enum_member() -> object:
    return COMPLEX_ENUM_MEMBER.value


def _stage_uses_callable() -> int:
    return _callable_helper()


@pytest.mark.parametrize(
    ("func", "var_name", "type_name"),
    [
        pytest.param(
            _stage_uses_mutable_dict,
            "MUTABLE_DICT",
            "dict",
            id="mutable-dict",
        ),
        pytest.param(
            _stage_uses_mutable_list,
            "MUTABLE_LIST",
            "list",
            id="mutable-list",
        ),
        pytest.param(
            _stage_uses_mutable_set,
            "MUTABLE_SET",
            "set",
            id="mutable-set",
        ),
        pytest.param(
            _stage_uses_mutable_dataclass,
            "MUTABLE_DATACLASS",
            "MutableConfig",
            id="mutable-dataclass",
        ),
        pytest.param(
            _stage_uses_mutable_pydantic,
            "MUTABLE_PYDANTIC",
            "MutableModel",
            id="mutable-pydantic",
        ),
    ],
)
def test_mutable_closure_capture_raises(
    func: Callable[[], int],
    var_name: str,
    type_name: str,
) -> None:
    with pytest.raises(exceptions.StageDefinitionError) as exc:
        fingerprint.get_stage_fingerprint_cached("train", func)

    message = str(exc.value)
    assert "Stage 'train'" in message, "Should include stage name"
    assert var_name in message, "Should include captured variable name"
    assert f"type: {type_name}" in message, "Should include captured variable type"
    assert "Fix: pass this data via StageParams" in message, "Should include suggestion"
    assert "PIVOT_UNSAFE_FINGERPRINTING=1" in message, "Should include suppression hint"


@pytest.mark.parametrize(
    "func",
    [
        pytest.param(_stage_uses_frozen_dataclass, id="frozen-dataclass"),
        pytest.param(_stage_uses_frozen_pydantic, id="frozen-pydantic"),
        pytest.param(_stage_uses_tuple, id="tuple"),
        pytest.param(_stage_uses_frozenset, id="frozenset"),
        pytest.param(_stage_uses_primitive, id="primitive"),
        pytest.param(_stage_uses_callable, id="callable"),
        pytest.param(_stage_uses_enum_member, id="enum-member"),
    ],
)
def test_immutable_closure_capture_allows_fingerprint(func: Callable[[], object]) -> None:
    fingerprint.get_stage_fingerprint_cached("train", func)


def test_enum_member_capture_is_tracked() -> None:
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_enum_member)
    assert manifest["enum:test_safe_fingerprinting.ENUM_MEMBER"] == "Basis.FRONTIER", (
        "Captured enum member should be tracked by class-qualified name"
    )


def test_callable_enum_member_capture_is_tracked() -> None:
    """An enum member is tracked via the enum path even when the enum defines __call__."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_callable_enum_member)
    assert manifest["enum:test_safe_fingerprinting.CALLABLE_ENUM_MEMBER"] == "CallableBasis.A", (
        "Callable enum member must be tracked by name, not id()-hashed as a callable"
    )
    assert f"func:{__name__}.CALLABLE_ENUM_MEMBER" not in manifest, (
        "Callable enum member must not fall through to the callable branch"
    )


def test_enum_member_with_unencodable_value_raises() -> None:
    """An enum whose value can't be soundly encoded errors instead of silently under-tracking."""
    with pytest.raises(exceptions.StageDefinitionError, match="cannot be soundly fingerprinted"):
        fingerprint.get_stage_fingerprint(_stage_uses_complex_enum_member)


def test_unsafe_env_allows_mutable_capture(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("PIVOT_UNSAFE_FINGERPRINTING", "1")
    with caplog.at_level(logging.WARNING):
        fingerprint.get_stage_fingerprint_cached("train", _stage_uses_mutable_dict)

    assert any(
        "closure captures mutable variable" in record.message for record in caplog.records
    ), "Should warn when unsafe fingerprinting is enabled"


NESTED_MUTABLE_TUPLE = (1, [2, 3])
INSTANCE_TUPLE = (MutableConfig(1),)
CALLABLE_TUPLE = (_callable_helper, _callable_helper)
ENUM_TUPLE = (Basis.FRONTIER, Basis.HEAD)
ENUM_FROZENSET = frozenset({Basis.FRONTIER})
FROZEN_DATACLASS_TUPLE = (FrozenConfig(value=1), FrozenConfig(value=2))
FROZEN_PYDANTIC_TUPLE = (FrozenModel(value=1),)
NESTED_FROZEN_TUPLE = (("first", FrozenConfig(value=1)), ("second", FrozenConfig(value=2)))


def _stage_uses_enum_tuple() -> int:
    return len(ENUM_TUPLE)


def _stage_uses_enum_frozenset() -> int:
    return len(ENUM_FROZENSET)


def _stage_uses_frozen_dataclass_tuple() -> int:
    return len(FROZEN_DATACLASS_TUPLE)


def _stage_uses_frozen_pydantic_tuple() -> int:
    return len(FROZEN_PYDANTIC_TUPLE)


def _stage_uses_nested_frozen_tuple() -> int:
    return len(NESTED_FROZEN_TUPLE)


def _stage_uses_nested_mutable_tuple() -> int:
    return len(NESTED_MUTABLE_TUPLE)


def _stage_uses_instance_tuple() -> int:
    return len(INSTANCE_TUPLE)


def _stage_uses_callable_tuple() -> int:
    return CALLABLE_TUPLE[0]()


def _stage_uses_mutable_dict_for_config_test() -> int:
    return MUTABLE_DICT["a"]


def test_unsafe_config_allows_mutable_capture(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Config-based unsafe_fingerprinting=true downgrades errors to warnings."""
    unsafe_config = models.PivotConfig.get_default()
    unsafe_config.core.unsafe_fingerprinting = True

    with (
        mock.patch("pivot.config.io.get_merged_config", autospec=True, return_value=unsafe_config),
        caplog.at_level(logging.WARNING),
    ):
        fingerprint.get_stage_fingerprint_cached("train", _stage_uses_mutable_dict_for_config_test)

    assert any(
        "closure captures mutable variable" in record.message for record in caplog.records
    ), "Should warn when unsafe fingerprinting via config is enabled"


def test_nested_mutable_in_tuple_raises() -> None:
    """A tuple nesting a mutable list is rejected: its contents can change at runtime."""
    with pytest.raises(exceptions.StageDefinitionError, match="nests a mutable list"):
        fingerprint.get_stage_fingerprint_cached("train", _stage_uses_nested_mutable_tuple)


def test_instance_in_tuple_raises() -> None:
    """A tuple holding a class instance is rejected rather than silently under-tracked."""
    with pytest.raises(exceptions.StageDefinitionError, match="element of type 'MutableConfig'"):
        fingerprint.get_stage_fingerprint_cached("train", _stage_uses_instance_tuple)


def test_callable_tuple_allows_fingerprint() -> None:
    """A tuple of callables (dispatch table) is still allowed; the callables are tracked."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_callable_tuple)
    assert f"func:{__name__}._callable_helper" in manifest, (
        "Callables inside the tuple should be tracked"
    )


def test_enum_tuple_allows_fingerprint_and_tracks_members() -> None:
    """A tuple of enum members is allowed (like a standalone enum) and each member is tracked."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_enum_tuple)
    assert "const:test_safe_fingerprinting.ENUM_TUPLE" in manifest, (
        "The tuple itself should be content-hashed"
    )
    assert manifest["enum:test_safe_fingerprinting.ENUM_TUPLE[0]"] == "Basis.FRONTIER"
    assert manifest["enum:test_safe_fingerprinting.ENUM_TUPLE[1]"] == "Basis.HEAD"


def test_enum_frozenset_allows_fingerprint_and_tracks_members() -> None:
    """A frozenset of enum members is allowed and its members are tracked by name."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_enum_frozenset)
    assert "const:test_safe_fingerprinting.ENUM_FROZENSET" in manifest, (
        "The frozenset itself should be content-hashed"
    )
    assert manifest["enum:test_safe_fingerprinting.ENUM_FROZENSET[0]"] == "Basis.FRONTIER"


def test_frozen_dataclass_tuple_allows_fingerprint_and_tracks_class() -> None:
    """A tuple of frozen dataclass instances is allowed, content-hashed, and its class tracked."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_frozen_dataclass_tuple)
    assert "const:test_safe_fingerprinting.FROZEN_DATACLASS_TUPLE" in manifest, (
        "The tuple should be content-hashed"
    )
    assert f"class:{__name__}.FrozenConfig" in manifest, "Element class should be tracked"


def test_frozen_pydantic_tuple_allows_fingerprint_and_tracks_class() -> None:
    """A tuple of frozen pydantic instances is allowed, content-hashed, and its class tracked."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_frozen_pydantic_tuple)
    assert "const:test_safe_fingerprinting.FROZEN_PYDANTIC_TUPLE" in manifest, (
        "The tuple should be content-hashed"
    )
    assert f"class:{__name__}.FrozenModel" in manifest, "Element class should be tracked"


def test_nested_frozen_instance_tuple_allows_fingerprint() -> None:
    """A tuple of (label, frozen-instance) tuples is allowed and content-hashed."""
    manifest = fingerprint.get_stage_fingerprint(_stage_uses_nested_frozen_tuple)
    assert "const:test_safe_fingerprinting.NESTED_FROZEN_TUPLE" in manifest, (
        "The nested tuple should be content-hashed"
    )
