# Fingerprint Tests

This directory contains all tests for Pivot's automatic code change detection (fingerprinting) system.

## Test Files

- **`test_fingerprint.py`** - Unit tests for fingerprinting functions
- **`test_integration.py`** - End-to-end tests with real Python files on disk
- **`test_change_detection.py`** - Comprehensive change detection behavior tests
- **`test_pydantic_defaults.py`** - Tests for Pydantic default data tracking
- **`test_functools.py`** - Tests for `functools.partial` and `functools.wraps` handling
- **`test_code_hash_portability.py`** - Tests that bytecode hashes ignore source file paths
- **`test_callback_vulnerabilities.py`** - Tests documenting callback detection edge cases
- **`test_determinism.py`** - Cross-process fingerprint stability tests (builtins, default_factory)
- **`test_safe_fingerprinting.py`** - Tests for safe fingerprinting error guards (mutable captures, dynamic name access) and immutable captures (frozen dataclasses, enums, tuples)

---

# Change Detection Matrix

This document exhaustively catalogs what code changes are and are not detected by Pivot's fingerprinting system, with test references.

**Legend:**

- ✅ **Detected** - Change triggers cache miss (correct behavior)
- ❌ **Not Detected** - Change does NOT trigger cache miss (limitation)
- 🚫 **Intentionally Ignored** - By design, change should NOT trigger cache miss
- ⚠️ **Partial** - Some cases detected, others not

---

## 1. Stage Function Itself

| Change Type                     | Detected? | Test Reference                                                        |
| ------------------------------- | --------- | --------------------------------------------------------------------- |
| Function body logic change      | ✅        | `test_change_detection.py::test_function_body_change_causes_miss`     |
| Function argument added/removed | ✅        | `test_change_detection.py::test_function_argument_change_causes_miss` |
| Default argument value change   | ✅        | `test_change_detection.py::test_default_value_change_causes_miss`     |
| Local variable rename           | ✅        | `test_change_detection.py::test_variable_rename_causes_miss`          |
| Parameter name change           | ✅        | `test_fingerprint.py::test_different_variable_names_different_hash`   |
| Type annotation change          | ✅        | `test_change_detection.py::test_type_annotation_change_causes_miss`   |
| Type alias definition change    | ⚠️        | Without `from __future__ import annotations`: alias is in globals, hashed by closure catch-all. With `from __future__`: alias only in string annotation, not captured. |
| Function name change            | 🚫        | `test_change_detection.py::test_function_rename_no_miss`              |
| Docstring change                | 🚫        | `test_change_detection.py::test_docstring_change_no_miss`             |
| Comment change                  | 🚫        | `test_change_detection.py::test_comment_change_no_miss`               |
| Whitespace change               | 🚫        | `test_change_detection.py::test_whitespace_change_no_miss`            |
| Decorator on stage function     | ❌        | **NO TEST** - decorators on the stage itself not tracked              |
| Async vs sync change            | ✅        | `test_change_detection.py::test_async_sync_change_causes_miss`        |

---

## 2. Same-Module Dependencies

| Change Type                                          | Detected? | Test Reference                                                                                                                      |
| ---------------------------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Helper function body change                          | ✅        | `test_fingerprint.py::test_helper_function_captured`                                                                                |
| Helper function via alias (`f = helper; f()`)        | ✅        | `test_fingerprint.py::test_aliased_function_captured`                                                                               |
| Transitive helper (helper's helper)                  | ✅        | `test_fingerprint.py::test_transitive_dependencies_captured`                                                                        |
| Global constant (int, float, str, bool, bytes, None) | ✅        | `test_fingerprint.py::test_constant_captured`, `test_fingerprint.py::test_multiple_constants_captured`                              |
| Global constant change                               | ✅        | `test_change_detection.py::test_global_constant_change_causes_miss`                                                                 |
| Global collection (list, dict, set) with callables   | ✅        | `test_change_detection.py::test_list_callable_tracking`, `test_change_detection.py::test_dispatch_dict_function_change_causes_miss` |
| Mutable collection DATA (dict/list/set values)        | ❌        | `test_pydantic_defaults.py::test_list_constants_not_captured_as_const`                                                               |
| Immutable collection DATA (tuple/frozenset of primitives) | ✅    | `test_change_detection.py::test_tuple_constant_content_change_causes_miss` (content-hashed)                                          |
| Immutable collection of enum members (tuple/frozenset) | ✅       | `test_safe_fingerprinting.py::test_enum_tuple_allows_fingerprint_and_tracks_members`, `test_safe_fingerprinting.py::test_enum_frozenset_allows_fingerprint_and_tracks_members`, `test_change_detection.py::test_enum_tuple_member_selection_change_causes_miss`, `test_change_detection.py::test_enum_tuple_member_value_change_causes_miss` (content-hashed + each member tracked like a standalone enum) |
| Immutable collection of frozen dataclass/pydantic instances | ✅ | `test_safe_fingerprinting.py::test_frozen_dataclass_tuple_allows_fingerprint_and_tracks_class`, `test_safe_fingerprinting.py::test_frozen_pydantic_tuple_allows_fingerprint_and_tracks_class`, `test_safe_fingerprinting.py::test_nested_frozen_instance_tuple_allows_fingerprint`, `test_change_detection.py::test_frozen_instance_tuple_field_change_causes_miss` (content-hashed + class tracked like a standalone frozen instance) |
| Immutable collection of MUTABLE class instances (tuple/frozenset) | ✅ (error) | `test_safe_fingerprinting.py::test_instance_in_tuple_raises` (still rejected — not immutable) |
| Enum member captured as global                        | ✅        | `test_safe_fingerprinting.py::test_enum_member_capture_is_tracked`, `test_change_detection.py::test_enum_member_selection_change_causes_miss`, `test_change_detection.py::test_enum_member_value_change_causes_miss` |
| Enum member value computed from a global              | ✅        | `test_change_detection.py::test_enum_value_from_global_change_causes_miss` (value hashed, not just class source)                     |
| Enum member with mutable (list) value                | ✅        | `test_change_detection.py::test_enum_mutable_value_change_causes_miss` (content-hashed)                                             |
| `IntFlag`/`Flag` pseudo-member (name=None) value     | ✅        | `test_change_detection.py::test_intflag_pseudo_member_value_change_causes_miss`                                                     |
| Functionally-created enum (`Enum("X", ...)`) value   | ✅        | `test_change_detection.py::test_functional_enum_value_change_causes_miss`, `test_determinism.py::test_functional_enum_capture_deterministic_across_processes` |
| Enum member with unencodable value                   | ✅ (error) | `test_safe_fingerprinting.py::test_enum_member_with_unencodable_value_raises`                                                       |
| `cached_property` / property setter transitive dep   | ✅        | `test_change_detection.py::test_cached_property_transitive_dependency_change_causes_miss`, `test_change_detection.py::test_property_setter_transitive_dependency_change_causes_miss` |
| Pydantic schema hash (model_json_schema)              | ✅        | `test_pydantic_defaults.py::test_pydantic_default_data_captured`                                                                     |
| Pydantic class in type hint                           | ✅        | `test_pydantic_defaults.py::test_pydantic_class_captured_from_type_hint`                                                             |
| Global class instance                                | ✅        | `test_change_detection.py::test_class_instance_tracked`, `test_change_detection.py::test_class_instance_change_causes_miss`         |
| Class definition change                              | ✅        | `test_change_detection.py::test_class_definition_change_causes_miss`                                                                |
| Class method change                                  | ✅        | `test_change_detection.py::test_class_definition_change_causes_miss` (class AST includes methods)                                   |
| Data class (or class) with user-defined methods      | ✅        | `test_fingerprint.py::test_data_class_with_methods_is_fingerprinted` (methods fingerprinted; transitive deps followed)             |
| Method's transitive dependency change                | ✅        | `test_change_detection.py::test_dataclass_method_transitive_dependency_change_causes_miss`                                          |
| Class base/field annotation dependencies             | ✅        | `test_fingerprint.py::test_process_class_body_dependencies_tracks_bases_and_annotations`                                            |
| StageParams `@property` method change                | ✅        | `test_change_detection.py::test_stageparams_property_change_causes_miss`                                                            |
| StageParams regular method change                    | ✅        | `test_change_detection.py::test_stageparams_method_change_causes_miss`                                                              |
| StageParams `ClassVar` change                        | ✅        | `test_change_detection.py::test_stageparams_class_variable_change_causes_miss`                                                      |
| Nested function body change                          | ✅        | `test_fingerprint.py::test_nested_function_not_in_globals` (detected via parent AST hash)                                           |
| Globals referenced only by nested functions          | ✅        | `test_change_detection.py::test_nested_function_global_reference_detected`, `test_change_detection.py::test_nested_function_global_change_causes_miss` |
| Helper starting with `_` prefix                      | ✅        | `test_change_detection.py::test_underscore_helper_change_detected`                                                                  |
| Stdlib callable in closure (e.g., `typing.cast`)     | 🚫        | `test_fingerprint.py::test_stdlib_callable_in_closure_does_not_raise`                                                              |
| `logging.Logger` in closure                          | 🚫        | Loggers are functionally irrelevant for cache invalidation                                                                          |
| Helper starting with `__` dunder                     | ❌        | `test_fingerprint.py::test_fingerprint_with_underscore_globals`                                                                     |

---

## 3. Cross-Module Dependencies (Module-Level Imports)

### 3.1 Direct Import (`from X import func`)

| Change Type                           | Detected? | Test Reference                                                               |
| ------------------------------------- | --------- | ---------------------------------------------------------------------------- |
| Imported function body change         | ✅        | `test_integration.py::test_direct_import_change_detected`                    |
| Imported function change (end-to-end) | ✅        | `test_change_detection.py::test_helper_via_direct_import_change_causes_miss` |
| Transitive dependency change          | ✅        | `test_integration.py::test_transitive_change_detected`                       |
| Imported constant                     | ✅        | `test_fingerprint.py::test_constant_captured` (same mechanism)               |
| Imported class                        | ✅        | `test_change_detection.py::test_class_definition_tracked_with_class_prefix`  |

### 3.2 Module Attribute (`import X; X.func()`)

| Change Type                         | Detected? | Test Reference                                                                          |
| ----------------------------------- | --------- | --------------------------------------------------------------------------------------- |
| Module function body change         | ✅        | `test_integration.py::test_module_attr_change_detected`                                 |
| Module function change (end-to-end) | ✅        | `test_change_detection.py::test_helper_via_module_attr_change_causes_miss`              |
| Transitive dependency change        | ✅        | `test_change_detection.py::test_transitive_dependency_change_causes_miss`               |
| Module constant                     | ✅        | `test_integration.py::test_constant_via_module_attr_captured`                           |
| Module constant captured            | ✅        | `test_change_detection.py::test_module_constant_captured_via_module_attr`               |
| Module immutable primitive collection (tuple/frozenset) | ✅ | `test_integration.py::test_immutable_primitive_module_collection_fingerprinting`, `test_integration.py::test_immutable_primitive_module_collection_change_detected` (content-hashed) |
| Module mutable collection (dict/list/set) DATA | ❌ (error) | `test_integration.py::test_mutable_module_collection_raises_error` (rejected via `_check_mutable_capture`, honoring `unsafe_fingerprinting`) |
| Module tuple/frozenset nesting mutable or instance | ✅ (error) | `test_integration.py::test_nested_mutable_in_module_tuple_raises`, `test_integration.py::test_instance_in_module_tuple_raises` (rejected via `_check_immutable_collection_capture`) |
| Module tuple/frozenset of callables (dispatch table) | ✅ | `test_integration.py::test_callable_module_tuple_tracks_callables` |
| Module tuple/frozenset of enum members | ✅ | content-hashed + each member tracked (shares `_process_collection_dependency` with the closure path) |
| Nested attribute (`X.sub.func`)     | ⚠️        | `test_fingerprint.py::test_extract_nested_attr_access` (extracted but not fully tested) |
| Multiple attrs from same module     | ✅        | `test_fingerprint.py::test_multiple_module_attrs_detected`                              |
| Both import styles in same stage    | ✅        | `test_integration.py::test_both_import_styles_in_same_stage`                            |

---

## 4. Cross-Module Dependencies (Lazy Imports Inside Function Body)

| Change Type                          | Detected? | Test Reference                                                       |
| ------------------------------------ | --------- | -------------------------------------------------------------------- |
| `from X import func` inside function | ❌        | `test_change_detection.py::test_lazy_import_change_detected` (xfail) |
| `import X` inside function           | ❌        | **NO TEST** - same limitation                                        |
| Conditional import inside function   | ❌        | **NO TEST** - same limitation                                        |

---

## 5. Special Naming Patterns

| Change Type                  | Detected? | Test Reference                                                     |
| ---------------------------- | --------- | ------------------------------------------------------------------ |
| Names starting with `_`      | ✅        | `test_change_detection.py::test_underscore_helper_change_detected` |
| Names starting with `__`     | ❌        | `test_fingerprint.py::test_fingerprint_skips_underscore_globals`   |
| `__name__`, `__file__`, etc. | 🚫        | `test_fingerprint.py::test_fingerprint_with_underscore_globals`    |

---

## 6. Stdlib and Third-Party

| Change Type                                | Detected? | Test Reference                                                            |
| ------------------------------------------ | --------- | ------------------------------------------------------------------------- |
| Stdlib function (e.g., `json.dumps`)       | 🚫        | `test_integration.py::test_stdlib_marked_callable_not_hashed`             |
| Stdlib via module attr                     | 🚫        | `test_change_detection.py::test_stdlib_function_marked_callable`          |
| Third-party function (e.g., `numpy.array`) | 🚫        | `test_fingerprint.py::test_is_user_code_non_user` (pytest.fixture tested) |
| Third-party package version change         | ❌        | **NO TEST** - versions not tracked                                        |
| Pivot framework code (`StageParams`, etc.) | 🚫        | `test_fingerprint.py::test_is_user_code_pivot_is_framework`               |
| Builtin function (`len`, `print`)          | 🚫        | `test_fingerprint.py::test_fingerprint_builtin_function_skipped`          |

---

## 7. Dynamic/Runtime Patterns

| Change Type                              | Detected? | Test Reference                                                                                                                                |
| ---------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `getattr(module, "func")`                | ❌        | **NO TEST** - can't resolve statically                                                                                                        |
| `getattr(obj, name)` (non-literal)       | ✅ (error) | `test_fingerprint.py::test_check_dynamic_name_access_rejects_dynamic_getattr`                                                        |
| `globals()` / `locals()`                 | ✅ (error) | `test_fingerprint.py::test_check_dynamic_name_access_rejects_globals`                                                                  |
| `func_dict[key]()` dispatch              | ✅        | `test_change_detection.py::test_dynamic_dispatch_change_detected`, `test_change_detection.py::test_dispatch_dict_function_change_causes_miss` |
| `eval()` / `exec()`                      | ❌        | **NO TEST** - can't analyze dynamically                                                                                                       |
| `importlib.import_module(string)`        | ❌        | **NO TEST** - can't resolve statically                                                                                                        |
| `importlib.import_module(...)` (call)    | ✅ (error) | `test_fingerprint.py::test_check_dynamic_name_access_rejects_import_module`                                                           |
| Method call on instance (`obj.method()`) | ❌        | `test_change_detection.py::test_class_instance_method_change_detected` (xfail)                                                                |
| Callback/function passed as argument     | ❌        | **NO TEST** - not tracked                                                                                                                     |
| Closure capturing outer variable         | ⚠️        | `test_fingerprint.py::test_fingerprint_with_nonlocal` (value captured, not source)                                                            |

---

## 8. Closures and Nonlocals

| Change Type                       | Detected? | Test Reference                                                     |
| --------------------------------- | --------- | ------------------------------------------------------------------ |
| Nonlocal primitive constant       | ✅        | `test_fingerprint.py::test_fingerprint_with_nonlocal`              |
| Nonlocal callable (user function) | ✅        | `test_fingerprint.py::test_fingerprint_nonlocal_callable_function` |
| Closure variable value change     | ✅        | `test_fingerprint.py::test_fingerprint_callable_nonlocal`          |
| Closure value with deterministic repr | ✅        | `test_fingerprint.py::test_fingerprint_captures_unrecognized_closure_value` |
| Closure value with `0x` in repr (non-deterministic) | 🚫  | `test_fingerprint.py::test_hash_unrecognized_closure_value_memory_address_raises` |
| Closure value with repr > 10KB        | 🚫        | `test_fingerprint.py::test_hash_unrecognized_closure_value_large_repr_raises` |

---

## 9. Edge Cases

| Change Type                               | Detected? | Test Reference                                                                  |
| ----------------------------------------- | --------- | ------------------------------------------------------------------------------- |
| Lambda function                           | ⚠️        | `test_fingerprint.py::test_lambda_function_fingerprinted` (hashed but unstable) |
| Function without source (builtin)         | ⚠️        | `test_fingerprint.py::test_hash_builtin_function` (fallback hash)               |
| Function without `__code__`               | ⚠️        | `test_fingerprint.py::test_hash_function_no_code_object` (id-based hash)        |
| Circular references                       | ✅        | `test_fingerprint.py::test_circular_reference_handled`                          |
| Recursive function                        | ✅        | Covered by circular reference handling                                          |
| `getclosurevars` raises exception         | ✅        | `test_fingerprint.py::test_fingerprint_getclosurevars_exception`                |
| Mixed resolvable/unresolvable type hints  | ✅        | `test_fingerprint.py::test_resolve_annotations_individually_mixed`              |
| Recursive Pydantic models (self-referential) | ✅     | `test_fingerprint.py::test_recursive_pydantic_model_no_infinite_recursion`      |
| Mutually recursive Pydantic models        | ✅        | `test_fingerprint.py::test_mutual_recursive_pydantic_models`                    |
| User-defined generic origin (`MyGeneric[int]`) | ✅   | `test_fingerprint.py::test_user_defined_generic_origin_tracked`                 |
| Dotted string forward ref in class body   | ✅        | `test_fingerprint.py::test_collect_annotation_names_dotted_string_forward_ref`  |
| Module not in sys.modules                 | ✅        | `test_fingerprint.py::test_is_user_code_module_not_in_sys_modules`              |
| Attribute doesn't exist on module         | ✅        | `test_fingerprint.py::test_module_attr_with_attribute_error`                    |
| Function with complex AST                 | ✅        | `test_fingerprint.py::test_hash_complex_ast`                                    |
| AST syntax error fallback                 | ✅        | `test_fingerprint.py::test_hash_function_syntax_error_fallback`                 |
| Child manifest merge (no duplicate self:) | ✅        | `test_fingerprint.py::test_fingerprint_merges_child_manifest`                   |

---

## 10. Hash Stability

| Property                                | Verified? | Test Reference                                                  |
| --------------------------------------- | --------- | --------------------------------------------------------------- |
| Same function → same hash               | ✅        | `test_fingerprint.py::test_unchanged_function_same_fingerprint` |
| Hash is deterministic                   | ✅        | `test_fingerprint.py::test_hash_is_stable`                      |
| Hash format (16-char hex)               | ✅        | `test_fingerprint.py::test_hash_format`                         |
| Multiple runs stable                    | ✅        | `test_change_detection.py::test_multiple_runs_stable`           |
| Same logic, different names → same hash | ✅        | `test_fingerprint.py::test_identical_functions_same_hash`       |
| Dict key order doesn't affect hash      | ✅        | `test_change_detection.py::test_fingerprint_ordering_stability` |
| Wrapped function hash independent of checkout path | ✅ | `test_code_hash_portability.py::test_wrapped_function_hash_independent_of_source_path` |
| `@contextlib.contextmanager` hash independent of interpreter install | ✅ | `test_code_hash_portability.py::test_contextmanager_hash_independent_of_interpreter_path` |
| Nested code object paths ignored        | ✅        | `test_code_hash_portability.py::test_hash_ignores_filename_of_nested_code_only` |

---

## 11. Loader Fingerprinting

| Change Type                     | Detected? | Test Reference                                                              |
| ------------------------------- | --------- | --------------------------------------------------------------------------- |
| Loader load() method change     | ✅        | `test_fingerprint.py::test_loader_fingerprint_includes_load_method`         |
| Loader save() method change     | ✅        | `test_fingerprint.py::test_loader_fingerprint_includes_save_method`         |
| Loader config field change      | ✅        | `test_fingerprint.py::test_loader_config_change_changes_fingerprint`        |
| Different loader types          | ✅        | `test_fingerprint.py::test_different_loader_types_different_fingerprint`    |
| Custom loader code change       | ✅        | `test_fingerprint.py::test_custom_loader_code_change_detected`              |
| Custom loader fingerprinting    | ✅        | `test_fingerprint.py::test_custom_loader_fingerprint`                       |
| Fingerprint stability           | ✅        | `test_fingerprint.py::test_loader_fingerprint_stable`                       |

---

## 12. Callback/Function Argument Detection

Tests in `test_callback_vulnerabilities.py` document edge cases where callback changes may not be detected.

| Change Type                                        | Detected? | Test Reference                                                                          |
| -------------------------------------------------- | --------- | --------------------------------------------------------------------------------------- |
| Module-level callback variable change              | ✅        | `test_callback_vulnerabilities.py::test_module_variable_callback_change_detected`       |
| Dict callback in closure                           | ✅        | `test_callback_vulnerabilities.py::test_dict_callback_in_closure_detected`              |
| Multi-layer wrapped callbacks via closure          | ✅        | `test_callback_vulnerabilities.py::test_multilayer_closure_callbacks_detected`          |
| Async/generator callback change                    | ✅        | `test_callback_vulnerabilities.py::test_async_callback_change_detected`                 |
| `@functools.wraps` decorator logic change          | ✅        | `test_functools.py::test_decorator_change_triggers_fingerprint_change`                  |
| Manual `__wrapped__` attribute detection           | ✅        | `test_callback_vulnerabilities.py::test_manual_wrapped_attribute_hides_code`            |
| `functools.partial` arguments tracked              | ✅        | `test_functools.py::test_partial_args_change_triggers_fingerprint_change`               |
| `functools.partial` wrapped function tracked       | ✅        | `test_functools.py::test_partial_underlying_func_change_triggers_fingerprint_change`    |
| Instance/container state callbacks not tracked     | ❌        | `test_callback_vulnerabilities.py::test_container_callback_change_detected`             |
| Class attribute callbacks (runtime modified)       | ❌        | `test_callback_vulnerabilities.py::test_class_attribute_callback_change_detected`       |

---

## 13. `@no_fingerprint` Decorator (File-Level Hashing)

When a stage uses `@pivot.no_fingerprint()`, AST fingerprinting is bypassed entirely. Change detection uses whole-file hashing instead.

| Change Type                                    | Detected? | Test Reference                                                                  |
| ---------------------------------------------- | --------- | ------------------------------------------------------------------------------- |
| Stage source file content change               | ✅        | `test_no_fingerprint.py::test_no_fingerprint_reruns_on_source_file_change`      |
| `code_deps` file content change                | ✅        | `test_no_fingerprint.py::test_no_fingerprint_reruns_on_code_deps_change`        |
| Unrelated file change (no re-run)              | ✅        | `test_no_fingerprint.py::test_no_fingerprint_skip_unrelated_change`             |
| Comment/whitespace change in source file       | ✅        | Not separately tested — file hash changes on any content change                 |
| Helper function in different file              | ❌        | Not detected unless listed in `code_deps`                                       |
| Missing `code_deps` file raises error          | ✅        | `test_no_fingerprint.py::test_no_fingerprint_missing_code_deps_file`            |
| Mixed pipeline (AST + no_fingerprint)          | ✅        | `test_no_fingerprint.py::test_no_fingerprint_mixed_pipeline`                    |
| Skip on unchanged (second run)                 | ✅        | `test_no_fingerprint.py::test_no_fingerprint_stage_skips_when_unchanged`        |

---

## Summary: Remaining Gaps

| Gap                                              | Priority | Difficulty                    |
| ------------------------------------------------ | -------- | ----------------------------- |
| Decorator on stage function not tracked          | Medium   | Easy                          |
| Nested module attribute (`X.sub.func`) full test | Low      | Easy                          |
| `import X` inside function not tracked           | Low      | Easy (already documented)     |
| `eval()`/`exec()` limitation                     | Low      | Easy                          |
| Third-party package version tracking             | Medium   | Hard (would need new feature) |
| Type alias change with `from __future__`         | Low      | Hard (alias only in string annotation, not in bytecode) |

---

## Design Decisions

1. **Dunder name filtering**: Names starting with `__` are skipped to filter `__name__`, `__file__`, `__doc__`, etc. Single-underscore names like `_private_helper()` are tracked normally.

2. **Stdlib/third-party marked "callable"**: These are not hashed because we don't want package version changes to trigger rebuilds (too sensitive).

3. **Function name normalization**: Function names are normalized to `"func"` in AST to allow renaming without cache miss.

4. **Docstrings/comments ignored**: These don't affect behavior, so they're stripped from AST.

5. **Lazy imports not tracked**: Imports inside function bodies are not detected. Recommended pattern: use module-level imports.

6. **Collection callable tracking**: Callables inside global collections (list, dict, set, tuple, frozenset) are detected and hashed. Dict keys are sorted alphabetically for deterministic ordering; sets are also sorted. For DATA (non-callable values): immutable collections (tuple, frozenset) containing only primitives are additionally content-hashed via a **type-tagged** canonical encoding (so `(1, 2)`, `[1, 2]`, and `frozenset({1, 2})` — and dict keys `1` vs `"1"`, `b"1"` vs `"1"` — hash differently, and nested sets/frozensets sort deterministically across processes). Mutable collections (dict, list, set) are NOT content-hashed and captured ones trigger `_check_mutable_capture`. A captured tuple/frozenset that **nests** a mutable collection, or contains an element that isn't a primitive, enum member, frozen dataclass/pydantic instance, callable, or nested tuple/frozenset (e.g. a mutable class instance), is rejected via `_check_immutable_collection_capture` (honoring `unsafe_fingerprinting`) rather than silently under-tracked. Enum members and frozen dataclass/pydantic instances held in a tuple/frozenset are allowed and each is tracked like a standalone capture (enum: identity + value + class source; frozen instance: class source, with field values captured by the collection's content hash) via `_process_collection_dependency`.

7. **Bytecode fallback uses marshal, with filenames stripped**: When source code is unavailable, `marshal.dumps(func.__code__)` captures the full code object including constants. This ensures that `return x + 1` and `return x + 999` produce different hashes (raw bytecode alone doesn't include constants). A marshalled code object also embeds `co_filename` — an absolute path — so `fingerprint.hash_code_object()` replaces the filename of the code object and every nested code constant with a fixed placeholder first. Without that, the same code hashes differently in every checkout and under every interpreter install, and lock files are only valid on the machine that wrote them.

    Tests: `test_code_hash_portability.py`

8. **Class definition tracking**: Classes are tracked using the `class:` prefix (e.g., `class:MyProcessor`). The entire class definition is hashed including all methods, class variables, and decorators. Module-level class instances (e.g., `processor = Processor()`) have their class type tracked via `class:varname.__class__`.

9. **Class body dependency tracking**: When `getclosurevars()` fails on a class, the class body is parsed to capture base classes and field annotations. Resolved user-code classes are fingerprinted transitively; third-party/stdlib bases are ignored.

10. **Callable instances tracked as functions**: Objects with `__call__` methods are matched by the `callable()` check before the instance check. This means changes to non-`__call__` methods on such classes may not trigger cache invalidation. Workaround: use the class directly instead of a pre-instantiated callable.

11. **NamedTuple instances tracked as tuples**: `NamedTuple` instances inherit from `tuple` and are matched by the collection check. Changes to the `NamedTuple` class definition may not be detected. Workaround: use regular classes or dataclasses instead.

12. **Pydantic schemas ARE tracked**: Pydantic model classes used in type hints are automatically detected and their JSON schemas are hashed via `model_json_schema()` (with titles/descriptions stripped to avoid false invalidation from docstring changes). The fingerprint includes:
    - `class:ModelName` - Hash of the class AST (captures structural changes including validators)
    - `schema:ModelName` - Hash of JSON schema + field defaults (captures field types, defaults, configuration)

    Nested model types are discovered by walking `model_fields` annotations, so changes to nested models propagate. A placeholder is inserted before field walking to prevent infinite recursion on self-referential models.

    Tests: `test_pydantic_defaults.py::test_pydantic_default_data_captured`, `test_pydantic_defaults.py::test_pydantic_class_captured_from_type_hint`, `test_pydantic_defaults.py::test_pydantic_default_change_triggers_different_hash`, `test_fingerprint.py::test_recursive_pydantic_model_no_infinite_recursion`

13. **`functools.wraps` / `__wrapped__` ARE tracked**: Functions decorated with `@functools.wraps` are properly fingerprinted using bytecode hashing. Since `inspect.getsource()` follows the `__wrapped__` chain and returns the original function's source, we detect `__wrapped__` and hash the wrapper's bytecode via `hash_code_object()` instead. This correctly captures decorator logic changes while closure analysis still tracks the wrapped function. Note that `@contextlib.contextmanager` applies `functools.wraps` itself, so contextmanager-decorated stages take this path too — with the wrapper's code object coming from contextlib inside the interpreter install.

    Tests: `test_functools.py::test_decorator_change_triggers_fingerprint_change`, `test_functools.py::test_wrapped_uses_bytecode_not_source`

14. **`functools.partial` IS tracked**: Partial objects are specially handled before the `is_user_code()` check. The fingerprint includes:
    - `partial:<name>.args` - Hash of bound positional arguments
    - `partial:<name>.kwargs` - Hash of bound keyword arguments
    - `func:<name>.func` - Hash of the underlying function (if user code)

    This allows Pivot to detect changes to both the partial's bound arguments and the underlying function.

    Tests: `test_functools.py::test_partial_is_detected`, `test_functools.py::test_partial_args_change_triggers_fingerprint_change`

15. **Instance state not tracked**: For user-defined class instances, only the class definition is hashed, not instance state (attributes, dict contents). Runtime-assigned callbacks on instances or in containers are not detected. Workaround: use module-level variables or explicit deps for mutable configuration.

16. **Class attributes modified at runtime not tracked**: Class attributes set after class definition (e.g., `Config.callback = func`) are not detected because only the original class source is hashed. Workaround: use module-level variables instead of class attributes for runtime configuration.

17. **StageParams classes fully tracked**: `StageParams` subclasses used in type hints are tracked via `_process_type_hint_dependencies()`. The entire class definition is hashed using `inspect.getsource()`, which captures:
    - `@property` methods
    - Regular methods
    - `ClassVar` declarations

    Changes to any of these trigger a cache miss.

    Tests: `test_change_detection.py::test_stageparams_property_change_causes_miss`, `test_change_detection.py::test_stageparams_method_change_causes_miss`, `test_change_detection.py::test_stageparams_class_variable_change_causes_miss`

18. **Builtin types ARE deterministic**: Builtin types (`list`, `dict`, `set`, `tuple`, etc.) used as `default_factory` in Pydantic fields are hashed using their qualified name (e.g., `builtin:list`) rather than `id()`. This ensures fingerprints are stable across Python sessions. Previously, `id()` was used as a fallback for objects without source code, causing spurious "Code changed" invalidations.

    Tests: `test_determinism.py::test_builtin_default_factory_deterministic_across_processes`, `test_determinism.py::test_builtin_type_deterministic`

19. **Module attribute collections use the SAME strictness as closure capture**: A collection reached as `mod.ATTR` is just a module-namespace entry — equally mutable at runtime as a same-module global — so it is treated identically to a captured global rather than as "more constant". A bare `dict`/`list`/`set` triggers `_check_mutable_capture` (error by default, warn under `unsafe_fingerprinting`) instead of being silently content-hashed. An immutable `tuple`/`frozenset` of primitives is content-hashed via the type-tagged canonical encoding; one that nests a mutable collection or holds a non-primitive/non-callable/non-enum element (e.g. a class instance) is rejected via `_check_immutable_collection_capture` (honoring `unsafe_fingerprinting`); a tuple/frozenset of callables (dispatch table) tracks each callable, and a tuple/frozenset of enum members tracks each member (identity + value + class source). Non-collection unsupported types (bare instances) still raise a `TypeError`. Previously the module path unconditionally content-hashed any `_is_primitive_collection` value (including bare mutable collections and tuples nesting mutable lists) and raised a bare `TypeError` for everything else, diverging from the closure path.

    Tests: `test_integration.py::test_immutable_primitive_module_collection_fingerprinting`, `test_integration.py::test_mutable_module_collection_raises_error`, `test_integration.py::test_nested_mutable_in_module_tuple_raises`, `test_integration.py::test_instance_in_module_tuple_raises`, `test_integration.py::test_callable_module_tuple_tracks_callables`

20. **Manifest cache invalidation is path-scoped**: Watch-mode reloads invalidate only the cached manifests whose source maps include changed paths, leaving unaffected stage manifests intact. Affected stages are recomputed on next fingerprint access and re-cached.

    Tests: `test_fingerprint.py::test_invalidate_manifests_for_paths_selective`, `test_fingerprint.py::test_invalidate_manifests_for_paths_recomputes_affected_stage`, `test_fingerprint.py::test_invalidate_manifests_for_paths_cold_start`

21. **Pivot framework excluded from `is_user_code`**: Modules in the `pivot` package are treated as framework code, not user code. This prevents framework internals (e.g., `StageParams`, `Out`, `Dep`) from appearing in manifests when using editable installs where pivot isn't in `site-packages`.

    Tests: `test_fingerprint.py::test_is_user_code_pivot_is_framework`

22. **Unrecognized closure values hashed via `repr()` with guards**: Values in closures that don't match known types (callables, modules, primitives, collections, enums, class instances) are hashed using `repr()` if the repr is deterministic (no `0x` memory addresses) and small (< 10KB). This catches datetime objects, regex patterns, etc. Non-deterministic or oversized reprs fall back to `_check_mutable_capture`.

23. **Enum members tracked as immutable**: An enum member captured as a global (bare `from mod import MEMBER` or qualified `mod.MEMBER`) is tracked by its class-qualified name (`enum:NAME` = `EnumClass.MEMBER`) AND by a canonical hash of its resolved value (`enum:NAME.value`). Hashing the value — not just the class source — catches values computed from other globals, `IntFlag`/`Flag` pseudo-members (whose `.name` is `None`), and functionally-created enums (`Enum("X", ...)`, which have no source). The enum class is additionally walked for its methods' transitive deps when it has source (skipped for source-less functional enums, which would otherwise hash non-deterministically). Enum members are treated as immutable-by-convention, like frozen dataclasses — repointing a constant to another member or editing a member's value both invalidate the stage. Values that are neither primitives nor collections of primitives raise `StageDefinitionError` (cannot be soundly encoded).

24. **Class/data-class methods are fingerprinted**: Methods defined on a class (including data classes) are walked as callables (`method:<module>.<qualname>.name`, module-qualified so same-named classes in different modules don't collide), so their transitive dependencies (helper functions/constants they call) invalidate dependents — the same guarantee standalone functions get. Data classes may therefore carry methods. Classmethods and staticmethods are unwrapped to their underlying function; `functools.cached_property` is unwrapped via `.func`; and a `property`'s getter (`method:Class.name`), setter (`method:Class.name.setter`), and deleter (`method:Class.name.deleter`) are each tracked, so a helper used only by, say, a setter still invalidates the stage. User-authored **behavioral dunders** (`__call__`, `__str__`, `__iter__`, `__post_init__`, operators, etc. — see `_FINGERPRINTED_DUNDERS`) are also walked; dataclass/pydantic-generated dunders (`__init__`, `__eq__`, ordering, `__hash__`, `__repr__`, …) are deliberately excluded to avoid churn from synthesized code.

    Tests: `test_fingerprint.py::test_hash_unrecognized_closure_value_deterministic_repr`, `test_fingerprint.py::test_hash_unrecognized_closure_value_memory_address_raises`, `test_fingerprint.py::test_fingerprint_captures_unrecognized_closure_value`

23. **Per-annotation type hint fallback**: When `typing.get_type_hints()` fails (e.g., one unresolvable `TYPE_CHECKING` import kills resolution for all annotations), each annotation string is evaluated individually via `eval()` in the function's global namespace. Resolvable annotations are still tracked; unresolvable ones are skipped.

    Tests: `test_fingerprint.py::test_resolve_annotations_individually_mixed`

24. **Non-user-code callables silently skipped**: Stdlib/third-party callables captured in closures (e.g., `typing.cast`, `json.dumps`) are skipped rather than hashed. Their `repr()` contains memory addresses (`0x...`) which would trigger false mutable-capture errors. These functions are already tracked via module dependency analysis when accessed as `module.func`.

    Tests: `test_fingerprint.py::test_stdlib_callable_in_closure_does_not_raise`

25. **Loggers silently skipped**: `logging.Logger` instances captured in closures are skipped. Module-level loggers (`_logger = logging.getLogger(__name__)`) are functionally irrelevant for cache invalidation — their repr can change based on logging configuration.

26. **User-defined generic origins tracked**: For generic types like `MyContainer[int]`, the origin class (`MyContainer`) is now fingerprinted if it's user code. Previously only the type arguments were walked.
