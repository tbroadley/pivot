import importlib
import pathlib
import sys
import types
from typing import TYPE_CHECKING

import pytest

from pivot import exceptions, fingerprint

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def module_dir(tmp_path: pathlib.Path) -> "Generator[pathlib.Path]":
    """Create a temporary directory for test modules and add to sys.path."""
    sys.path.insert(0, str(tmp_path))
    yield tmp_path
    sys.path.remove(str(tmp_path))
    # Clean up any modules we imported from this directory
    to_remove = [name for name in sys.modules if name.startswith("test_mod_")]
    for name in to_remove:
        del sys.modules[name]


def _import_module(name: str) -> types.ModuleType:
    """Import a module by name, handling cache invalidation."""
    importlib.invalidate_caches()
    return importlib.import_module(name)


def _reimport_module(name: str) -> types.ModuleType:
    """Re-import a module after clearing from sys.modules."""
    if name in sys.modules:
        del sys.modules[name]
    return _import_module(name)


def test_module_attr_change_detected(module_dir: pathlib.Path) -> None:
    """Changing helper accessed via module.attr changes fingerprint."""
    # Write helper module V1
    helpers_py = module_dir / "test_mod_helpers_v1.py"
    helpers_py.write_text("""
def process(x):
    return x * 2
""")

    # Write stage module that uses Google-style import
    stage_py = module_dir / "test_mod_stage_v1.py"
    stage_py.write_text("""
import test_mod_helpers_v1 as helpers

def run_stage():
    return helpers.process(10)
""")

    # Import and get fingerprint V1
    stage_mod = _import_module("test_mod_stage_v1")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Verify helper is captured with a hash (not "callable")
    helper_key = "mod:helpers.process"
    assert helper_key in fp1, f"Should capture helper.process, got: {fp1.keys()}"
    hash1 = fp1[helper_key]
    assert len(hash1) == 16, f"Should be 16-char hash, got: {hash1}"
    assert all(c in "0123456789abcdef" for c in hash1), f"Should be hex, got: {hash1}"

    # Modify helper function
    helpers_py.write_text("""
def process(x):
    return x * 3  # CHANGED!
""")

    # Force re-import
    _reimport_module("test_mod_helpers_v1")
    stage_mod_v2 = _reimport_module("test_mod_stage_v1")
    fp2 = fingerprint.get_stage_fingerprint(stage_mod_v2.run_stage)

    # Fingerprint MUST be different
    hash2 = fp2[helper_key]
    assert hash1 != hash2, f"Fingerprint must change when helper changes: {hash1} vs {hash2}"


def test_unchanged_code_same_fingerprint(module_dir: pathlib.Path) -> None:
    """Same code produces same fingerprint (stability check)."""
    helpers_py = module_dir / "test_mod_helpers_v2.py"
    helpers_py.write_text("""
def process(x):
    return x * 2
""")

    stage_py = module_dir / "test_mod_stage_v2.py"
    stage_py.write_text("""
import test_mod_helpers_v2 as helpers

def run_stage():
    return helpers.process(10)
""")

    stage_mod = _import_module("test_mod_stage_v2")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Get fingerprint again without changes
    fp2 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    assert fp1 == fp2, "Same code must produce same fingerprint"


def test_transitive_change_detected(module_dir: pathlib.Path) -> None:
    """Changing transitive dependency changes fingerprint."""
    # Write leaf module
    leaf_py = module_dir / "test_mod_leaf_v3.py"
    leaf_py.write_text("""
def leaf_func(x):
    return x + 1
""")

    # Write helper that uses leaf
    helpers_py = module_dir / "test_mod_helpers_v3.py"
    helpers_py.write_text("""
import test_mod_leaf_v3 as leaf

def process(x):
    return leaf.leaf_func(x) * 2
""")

    # Write stage that uses helper
    stage_py = module_dir / "test_mod_stage_v3.py"
    stage_py.write_text("""
import test_mod_helpers_v3 as helpers

def run_stage():
    return helpers.process(10)
""")

    stage_mod = _import_module("test_mod_stage_v3")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Verify transitive dep is captured
    assert "mod:leaf.leaf_func" in fp1, f"Should capture transitive dep, got: {fp1.keys()}"

    # Modify leaf function
    leaf_py.write_text("""
def leaf_func(x):
    return x + 100  # CHANGED!
""")

    # Force re-import of all modules
    for mod_name in ["test_mod_leaf_v3", "test_mod_helpers_v3", "test_mod_stage_v3"]:
        _reimport_module(mod_name)

    stage_mod_v2 = _import_module("test_mod_stage_v3")
    fp2 = fingerprint.get_stage_fingerprint(stage_mod_v2.run_stage)

    # Transitive fingerprint must change
    assert fp1["mod:leaf.leaf_func"] != fp2["mod:leaf.leaf_func"], (
        "Transitive dep fingerprint must change"
    )


def test_stdlib_module_attrs_not_tracked(module_dir: pathlib.Path) -> None:
    """Stdlib module attributes are NOT tracked in fingerprint."""
    stage_py = module_dir / "test_mod_stage_v4.py"
    stage_py.write_text("""
import json as json_mod

def run_stage():
    return json_mod.dumps({"key": "value"})
""")

    stage_mod = _import_module("test_mod_stage_v4")
    fp = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # json is stdlib - should NOT be in fingerprint
    assert "mod:json_mod.dumps" not in fp, (
        f"Stdlib module attrs should not be tracked, got: {list(fp.keys())}"
    )


def test_direct_import_change_detected(module_dir: pathlib.Path) -> None:
    """Changing directly imported function changes fingerprint."""
    # Write helper module V1
    helpers_py = module_dir / "test_mod_helpers_v5.py"
    helpers_py.write_text("""
def helper_func(x):
    return x * 2
""")

    # Write stage that uses direct import
    stage_py = module_dir / "test_mod_stage_v5.py"
    stage_py.write_text("""
from test_mod_helpers_v5 import helper_func

def run_stage():
    return helper_func(10)
""")

    stage_mod = _import_module("test_mod_stage_v5")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Verify helper is captured
    assert "func:helper_func" in fp1, f"Should capture helper_func, got: {fp1.keys()}"
    hash1 = fp1["func:helper_func"]

    # Modify helper
    helpers_py.write_text("""
def helper_func(x):
    return x * 3  # CHANGED!
""")

    # Force re-import
    _reimport_module("test_mod_helpers_v5")
    stage_mod_v2 = _reimport_module("test_mod_stage_v5")
    fp2 = fingerprint.get_stage_fingerprint(stage_mod_v2.run_stage)

    # Fingerprint must change
    hash2 = fp2["func:helper_func"]
    assert hash1 != hash2, f"Direct import fingerprint must change: {hash1} vs {hash2}"


def test_constant_via_module_attr_captured(module_dir: pathlib.Path) -> None:
    """Constants accessed via module.attr are captured."""
    helpers_py = module_dir / "test_mod_helpers_v6.py"
    helpers_py.write_text("""
THRESHOLD = 0.5

def process(x):
    return x > THRESHOLD
""")

    stage_py = module_dir / "test_mod_stage_v6.py"
    stage_py.write_text("""
import test_mod_helpers_v6 as helpers

def run_stage():
    return helpers.process(0.6) and helpers.THRESHOLD < 1.0
""")

    stage_mod = _import_module("test_mod_stage_v6")
    fp = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Constant should be captured with repr value
    assert "mod:helpers.THRESHOLD" in fp, f"Should capture constant, got: {fp.keys()}"
    assert fp["mod:helpers.THRESHOLD"] == "0.5", (
        f"Constant value should be repr, got: {fp['mod:helpers.THRESHOLD']}"
    )


def test_both_import_styles_in_same_stage(module_dir: pathlib.Path) -> None:
    """Stage using both direct and Google-style imports works."""
    helpers_py = module_dir / "test_mod_helpers_v7.py"
    helpers_py.write_text("""
def func_a(x):
    return x + 1

def func_b(x):
    return x * 2
""")

    stage_py = module_dir / "test_mod_stage_v7.py"
    stage_py.write_text("""
from test_mod_helpers_v7 import func_a
import test_mod_helpers_v7 as helpers

def run_stage():
    return func_a(10) + helpers.func_b(20)
""")

    stage_mod = _import_module("test_mod_stage_v7")
    fp = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    # Both should be captured
    assert "func:func_a" in fp, "Should capture direct import"
    assert "mod:helpers.func_b" in fp, "Should capture module attr"

    # Both should be hashes (not "callable")
    assert len(fp["func:func_a"]) == 16, "Direct import should be hashed"
    assert len(fp["mod:helpers.func_b"]) == 16, "Module attr should be hashed"


def test_mutable_module_collection_raises_error(
    module_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bare dict/list/set reached via module attr is rejected, mirroring closure capture.

    A `mod.ATTR` collection is just a module-namespace entry, equally mutable at runtime as a
    same-module global, so it goes through `_check_mutable_capture` rather than being silently
    content-hashed.
    """
    for suffix, decl in (
        ("dict", "MY_COLL = {'agent1': 'config1', 'agent2': 'config2'}"),
        ("list", "MY_COLL = [1, 2, 3, 4, 5]"),
        ("set", "MY_COLL = {1, 2, 3}"),
    ):
        helpers_py = module_dir / f"test_mod_helpers_mut_{suffix}.py"
        helpers_py.write_text(decl + "\n")
        stage_py = module_dir / f"test_mod_stage_mut_{suffix}.py"
        stage_py.write_text(f"""
import test_mod_helpers_mut_{suffix} as helpers

def run_stage():
    return helpers.MY_COLL
""")
        stage_mod = _import_module(f"test_mod_stage_mut_{suffix}")
        with pytest.raises(exceptions.StageDefinitionError) as exc:
            fingerprint.get_stage_fingerprint(stage_mod.run_stage)
        message = str(exc.value)
        assert "helpers.MY_COLL" in message, "Should name the module attribute"
        assert f"type: {suffix}" in message, "Should include the captured type"

    monkeypatch.setenv("PIVOT_UNSAFE_FINGERPRINTING", "1")
    helpers_py = module_dir / "test_mod_helpers_mut_unsafe.py"
    helpers_py.write_text("MY_COLL = {'a': 1}\n")
    stage_py = module_dir / "test_mod_stage_mut_unsafe.py"
    stage_py.write_text("""
import test_mod_helpers_mut_unsafe as helpers

def run_stage():
    return helpers.MY_COLL
""")
    stage_mod = _import_module("test_mod_stage_mut_unsafe")
    manifest = fingerprint.get_stage_fingerprint(stage_mod.run_stage)
    assert "mod:helpers.MY_COLL" not in manifest, (
        "Mutable collection must not be content-hashed even under unsafe fingerprinting"
    )


def test_immutable_primitive_module_collection_fingerprinting(module_dir: pathlib.Path) -> None:
    """Immutable primitive collections (tuple/frozenset) reached via module attr are content-hashed."""
    helpers_py = module_dir / "test_mod_helpers_v9.py"
    helpers_py.write_text("""
AGENTS = (("agent1", "config1"), ("agent2", "config2"))
NUMBERS = (1, 2, 3, 4, 5)
NESTED = ((1, 2), (3, (4, 5)))
""")

    stage_py = module_dir / "test_mod_stage_v9.py"
    stage_py.write_text("""
import test_mod_helpers_v9 as helpers

def run_stage():
    return helpers.AGENTS + helpers.NUMBERS + helpers.NESTED
""")

    stage_mod = _import_module("test_mod_stage_v9")
    manifest = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    for key in ["mod:helpers.AGENTS", "mod:helpers.NUMBERS", "mod:helpers.NESTED"]:
        assert key in manifest
        assert len(manifest[key]) == 16  # xxhash64 hex digest length


def test_immutable_primitive_module_collection_change_detected(module_dir: pathlib.Path) -> None:
    """Changing an immutable primitive collection module attribute changes the fingerprint."""
    helpers_py = module_dir / "test_mod_helpers_v10.py"
    helpers_py.write_text("""
AGENTS = (("agent1", "config1"), ("agent2", "config2"))
""")

    stage_py = module_dir / "test_mod_stage_v10.py"
    stage_py.write_text("""
import test_mod_helpers_v10 as helpers

def run_stage():
    return helpers.AGENTS
""")

    stage_mod = _import_module("test_mod_stage_v10")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)
    hash1 = fp1["mod:helpers.AGENTS"]

    helpers_py.write_text("""
AGENTS = (("agent1", "config1"), ("agent3", "config3"))  # CHANGED!
""")

    _reimport_module("test_mod_helpers_v10")
    stage_mod_v2 = _reimport_module("test_mod_stage_v10")
    fp2 = fingerprint.get_stage_fingerprint(stage_mod_v2.run_stage)
    hash2 = fp2["mod:helpers.AGENTS"]

    assert hash1 != hash2, f"Fingerprint must change when collection changes: {hash1} vs {hash2}"


def test_immutable_primitive_module_collection_deterministic(module_dir: pathlib.Path) -> None:
    """Same immutable primitive collection produces the same fingerprint (stability check)."""
    helpers_py = module_dir / "test_mod_helpers_v11.py"
    helpers_py.write_text("""
TUPLE_DATA = ("z", "a", "m")
FROZENSET_DATA = frozenset([3, 1, 4, 1, 5])  # Frozenset (unordered, with duplicate)
NESTED_DATA = (frozenset({"b", "a"}), (1, 2))
""")

    stage_py = module_dir / "test_mod_stage_v11.py"
    stage_py.write_text("""
import test_mod_helpers_v11 as helpers

def run_stage():
    return helpers.TUPLE_DATA and helpers.FROZENSET_DATA and helpers.NESTED_DATA
""")

    stage_mod = _import_module("test_mod_stage_v11")
    fp1 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)
    fp2 = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    assert fp1["mod:helpers.TUPLE_DATA"] == fp2["mod:helpers.TUPLE_DATA"]
    assert fp1["mod:helpers.FROZENSET_DATA"] == fp2["mod:helpers.FROZENSET_DATA"]
    assert fp1["mod:helpers.NESTED_DATA"] == fp2["mod:helpers.NESTED_DATA"]


def test_immutable_module_collection_edge_cases(module_dir: pathlib.Path) -> None:
    """Empty, deeply nested, and large immutable primitive collections are supported."""
    helpers_py = module_dir / "test_mod_helpers_v12.py"
    helpers_py.write_text("""
EMPTY_TUPLE = ()
EMPTY_FROZENSET = frozenset()
DEEPLY_NESTED = ("a", ("b", ("c", ("d", (1, 2, ("e", "value"))))))
LARGE_TUPLE = tuple(range(200))  # 200 elements
""")

    stage_py = module_dir / "test_mod_stage_v12.py"
    stage_py.write_text("""
import test_mod_helpers_v12 as helpers

def run_stage():
    return (
        helpers.EMPTY_TUPLE
        or helpers.EMPTY_FROZENSET
        or helpers.DEEPLY_NESTED
        or helpers.LARGE_TUPLE
    )
""")

    stage_mod = _import_module("test_mod_stage_v12")
    manifest = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    for key in [
        "mod:helpers.EMPTY_TUPLE",
        "mod:helpers.EMPTY_FROZENSET",
        "mod:helpers.DEEPLY_NESTED",
        "mod:helpers.LARGE_TUPLE",
    ]:
        assert key in manifest
        assert len(manifest[key]) == 16


def test_all_primitive_types_in_immutable_module_collections(module_dir: pathlib.Path) -> None:
    """All primitive types (bool/int/float/str/bytes/None) in tuple/frozenset are supported."""
    helpers_py = module_dir / "test_mod_helpers_v13.py"
    helpers_py.write_text("""
TUPLE_ALL_TYPES = (False, -1, -2.5, "tuple", b"data", None)
FROZENSET_PRIMITIVES = frozenset([True, False, 0, 1])
""")

    stage_py = module_dir / "test_mod_stage_v13.py"
    stage_py.write_text("""
import test_mod_helpers_v13 as helpers

def run_stage():
    return helpers.TUPLE_ALL_TYPES or helpers.FROZENSET_PRIMITIVES
""")

    stage_mod = _import_module("test_mod_stage_v13")
    manifest = fingerprint.get_stage_fingerprint(stage_mod.run_stage)

    for key in ["mod:helpers.TUPLE_ALL_TYPES", "mod:helpers.FROZENSET_PRIMITIVES"]:
        assert key in manifest
        assert len(manifest[key]) == 16


def test_nested_mutable_in_module_tuple_raises(module_dir: pathlib.Path) -> None:
    """A module-attr tuple nesting a mutable list is rejected: its contents can change at runtime."""
    helpers_py = module_dir / "test_mod_helpers_nested_mut.py"
    helpers_py.write_text("NESTED = (1, [2, 3])\n")
    stage_py = module_dir / "test_mod_stage_nested_mut.py"
    stage_py.write_text("""
import test_mod_helpers_nested_mut as helpers

def run_stage():
    return len(helpers.NESTED)
""")
    stage_mod = _import_module("test_mod_stage_nested_mut")
    with pytest.raises(exceptions.StageDefinitionError, match="nests a mutable list"):
        fingerprint.get_stage_fingerprint(stage_mod.run_stage)


def test_instance_in_module_tuple_raises(module_dir: pathlib.Path) -> None:
    """A module-attr tuple holding a class instance is rejected rather than raising a bare TypeError."""
    helpers_py = module_dir / "test_mod_helpers_inst_tuple.py"
    helpers_py.write_text("""
class Config:
    def __init__(self) -> None:
        self.value = 1

INSTANCE_TUPLE = (Config(),)
""")
    stage_py = module_dir / "test_mod_stage_inst_tuple.py"
    stage_py.write_text("""
import test_mod_helpers_inst_tuple as helpers

def run_stage():
    return len(helpers.INSTANCE_TUPLE)
""")
    stage_mod = _import_module("test_mod_stage_inst_tuple")
    with pytest.raises(exceptions.StageDefinitionError, match="element of type 'Config'"):
        fingerprint.get_stage_fingerprint(stage_mod.run_stage)


def test_callable_module_tuple_tracks_callables(module_dir: pathlib.Path) -> None:
    """A module-attr tuple of callables (dispatch table) is allowed; the callables are tracked."""
    helpers_py = module_dir / "test_mod_helpers_call_tuple.py"
    helpers_py.write_text("""
def _handler_a():
    return 1

def _handler_b():
    return 2

HANDLERS = (_handler_a, _handler_b)
""")
    stage_py = module_dir / "test_mod_stage_call_tuple.py"
    stage_py.write_text("""
import test_mod_helpers_call_tuple as helpers

def run_stage():
    return helpers.HANDLERS[0]()
""")
    stage_mod = _import_module("test_mod_stage_call_tuple")
    manifest = fingerprint.get_stage_fingerprint(stage_mod.run_stage)
    assert any(k.startswith("func:helpers.HANDLERS[") for k in manifest), (
        "Callables inside the module-attr tuple should be tracked"
    )


def test_mutable_dict_of_callables_via_module_attr_raises(module_dir: pathlib.Path) -> None:
    """A bare dict of callables is rejected as a mutable capture, matching the closure path."""
    helpers_py = module_dir / "test_mod_helpers_callbacks.py"
    helpers_py.write_text("""
def callback():
    return None

CALLBACKS = {"func": callback}
""")
    stage_py = module_dir / "test_mod_stage_callbacks.py"
    stage_py.write_text("""
import test_mod_helpers_callbacks as helpers

def run_stage():
    return helpers.CALLBACKS
""")
    stage_mod = _import_module("test_mod_stage_callbacks")
    with pytest.raises(exceptions.StageDefinitionError, match="type: dict"):
        fingerprint.get_stage_fingerprint(stage_mod.run_stage)


def test_unsupported_non_collection_module_attr_raises_type_error(module_dir: pathlib.Path) -> None:
    """A non-collection unsupported module attr (bare instance) still raises a clear TypeError."""
    helpers_py = module_dir / "test_mod_helpers_inst.py"
    helpers_py.write_text("""
class Config:
    def __init__(self) -> None:
        self.value = 1

CONFIG = Config()
""")
    stage_py = module_dir / "test_mod_stage_inst.py"
    stage_py.write_text("""
import test_mod_helpers_inst as helpers

def run_stage():
    return helpers.CONFIG.value
""")
    stage_mod = _import_module("test_mod_stage_inst")
    with pytest.raises(TypeError, match="Cannot fingerprint module attribute"):
        fingerprint.get_stage_fingerprint(stage_mod.run_stage)


def test_self_referential_module_list_raises(module_dir: pathlib.Path) -> None:
    """A self-referential (mutable) module-level list is rejected without infinite recursion."""
    helpers_py = module_dir / "test_mod_helpers_circular.py"
    helpers_py.write_text("""
CIRCULAR = [1, 2, 3]
CIRCULAR.append(CIRCULAR)
""")

    stage_py = module_dir / "test_mod_stage_circular.py"
    stage_py.write_text("""
import test_mod_helpers_circular as helpers

def run_stage():
    return helpers.CIRCULAR[0]
""")

    stage_mod = _import_module("test_mod_stage_circular")
    with pytest.raises(exceptions.StageDefinitionError, match="type: list"):
        fingerprint.get_stage_fingerprint(stage_mod.run_stage)
