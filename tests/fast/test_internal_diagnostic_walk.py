"""The walk that finds the code objects of the loaded project modules (``held_code``) runs no code of an object and never changes the exit code of a job.

Failure modes covered here (each found by the review of the walk):

- An exact Python function can hold a tuple subclass in ``co_consts`` or in ``__closure__``. Iterating or truth-testing it runs the subclass's ``__iter__``,
  ``__len__`` or ``__bool__``, and a ``SystemExit`` raised there ended the job with that code instead of 70. The walk now reads only an exact ``tuple``.
- The cached function that ``functools.cache`` makes keeps ``__wrapped__`` in its dictionary. A dictionary lookup runs the ``__eq__`` of a key whose hash is
  the same, so a custom key could run code. The walk scans the dictionary for an exact ``str`` key and compares nothing else.
- Any ``BaseException`` that the walk raises (``SystemExit`` and the like, not only ``Exception``) ends in the fixed unavailable line, and the caller still
  exits 70.
- The limit of the walk counts every inspected entry (a function, a code object, a constant, a namespace entry), and a namespace is read lazily: with a limit of
  1 a function that holds two nested code objects registers at most 1 code object, and a very large namespace is not copied before its entries are counted.
"""

from __future__ import annotations

import argparse
import functools
import sys
import tracemalloc
import types
import unittest
from typing import Any
from unittest import mock

from spark import internal_diagnostic as diagnostic
from spark import result_file
from spark.internal_diagnostic import UNAVAILABLE_LINE, held_code, internal_error_line
from tests.fast.internal_line_support import OWN_FILE, TRACKED, stand_in
from tests.fast.test_internal_diagnostic_rebuilt import REAL, caught

EXIT = 42
NAMED = ["other", f"{TRACKED}:{REAL}"]


class Exits(tuple):  # type: ignore[type-arg]
    """A tuple whose every hook ends the process with a code that is not 70."""

    def __iter__(self) -> Any:
        raise SystemExit(EXIT)

    def __len__(self) -> int:
        raise SystemExit(EXIT)

    def __bool__(self) -> bool:
        raise SystemExit(EXIT)

    def __getitem__(self, index: Any) -> Any:
        raise SystemExit(EXIT)


def raising() -> Any:
    raise KeyError


def with_constants(code: types.CodeType, constants: tuple[Any, ...]) -> types.FunctionType:
    return types.FunctionType(code.replace(co_consts=constants), {})


def error_of(scope: dict[str, Any]) -> BaseException:
    return caught(scope[REAL])


class TheTupleSubclass(unittest.TestCase):
    def scope(self) -> dict[str, Any]:
        scope: dict[str, Any] = {}
        exec(compile(f"def {REAL}():\n    raise KeyError\n", OWN_FILE, "exec"), scope)  # noqa: S102
        stand_in(self, OWN_FILE, scope)
        return scope

    def test_a_tuple_subclass_in_the_constants_of_a_held_function_runs_no_hook(self) -> None:
        scope = self.scope()
        hostile = with_constants(raising.__code__, Exits(raising.__code__.co_consts))
        with mock.patch.dict(sys.modules["spark.result_file"].__dict__, {"_hostile": hostile}):
            held = held_code()
            line = internal_error_line(error_of(scope))
        self.assertIn(id(hostile.__code__), held)
        self.assertEqual(NAMED, line.partition(" frames=")[2].split(" | "))

    def test_a_tuple_subclass_in_the_closure_of_a_held_function_runs_no_hook(self) -> None:
        scope = self.scope()
        value = 1

        def make() -> Any:
            def inner() -> int:
                return value

            return inner

        inner = make()
        hostile = types.FunctionType(inner.__code__, {}, None, None, Exits(inner.__closure__ or ()))
        with mock.patch.dict(sys.modules["spark.result_file"].__dict__, {"_hostile": hostile}):
            held = held_code()
            line = internal_error_line(error_of(scope))
        self.assertIn(id(hostile.__code__), held)
        self.assertEqual(NAMED, line.partition(" frames=")[2].split(" | "))

    def test_a_tuple_subclass_in_the_constants_of_nested_code_runs_no_hook(self) -> None:
        scope = self.scope()
        outer = compile("def f():\n    def g():\n        return 1\n    return g\n", OWN_FILE, "exec")
        function = next(item for item in outer.co_consts if type(item) is types.CodeType)
        hostile = with_constants(function, Exits(function.co_consts))
        with mock.patch.dict(sys.modules["spark.result_file"].__dict__, {"_hostile": hostile}):
            held_code()
            self.assertEqual(NAMED, internal_error_line(error_of(scope)).partition(" frames=")[2].split(" | "))


class TheCachedWrapper(unittest.TestCase):
    def test_a_custom_key_in_the_dictionary_of_a_cached_function_runs_no_eq(self) -> None:
        calls: list[str] = []
        armed: list[int] = []  # empty while the test builds the dictionary

        class Key:
            def __hash__(self) -> int:
                return hash("__wrapped__")

            def __eq__(self, other: object) -> bool:
                if armed:
                    calls.append("eq")
                    raise SystemExit(EXIT)
                return self is other

        cached = functools.cache(raising)
        namespace: dict[Any, Any] = cached.__dict__
        wrapped = namespace.pop("__wrapped__")
        namespace[Key()] = 1  # a key with the same hash as the name, placed first so that a lookup reaches it
        namespace["__wrapped__"] = wrapped
        armed.append(1)
        with mock.patch.dict(sys.modules["spark.result_file"].__dict__, {"_cached": cached}):
            held = held_code()
        self.assertEqual([], calls)
        self.assertIn(id(raising.__code__), held)  # the wrapped function is still found

    def test_the_wrapped_function_is_found_through_an_exact_str_key_only(self) -> None:
        class Name(str):
            pass

        cached = functools.cache(raising)
        namespace: dict[Any, Any] = cached.__dict__
        namespace[Name("__wrapped__")] = namespace.pop("__wrapped__")
        with mock.patch.dict(sys.modules["spark.result_file"].__dict__, {"_cached": cached}):
            self.assertNotIn(id(raising.__code__), held_code())


class TheExitCode(unittest.TestCase):
    def test_a_base_exception_from_the_walk_gives_the_unavailable_line(self) -> None:
        for raised in (SystemExit(EXIT), KeyboardInterrupt(), type("Custom", (BaseException,), {})()):
            with self.subTest(raised=type(raised).__name__), mock.patch.object(diagnostic, "held_code", side_effect=raised):
                scope: dict[str, Any] = {}
                exec(compile(f"def {REAL}():\n    raise KeyError\n", OWN_FILE, "exec"), scope)  # noqa: S102
                self.assertEqual(UNAVAILABLE_LINE, internal_error_line(caught(scope[REAL])))

    def test_a_job_whose_walk_ends_the_process_still_exits_70_with_the_unavailable_line(self) -> None:
        def execute(_: argparse.Namespace) -> dict[str, Any]:
            raise KeyError

        with mock.patch.object(diagnostic, "held_code", side_effect=SystemExit(EXIT)):
            status, document, lines = result_file.ending(argparse.Namespace(mode="m", branch="b"), execute, "job")
        self.assertEqual(diagnostic.INTERNAL_EXIT_CODE, status)
        self.assertEqual([{"code": "internal_error"}], document["diagnostics"])
        self.assertEqual(UNAVAILABLE_LINE, lines[-1])


class TheBudget(unittest.TestCase):
    def only(self, namespace: dict[str, Any]) -> Any:
        module = types.ModuleType("spark.result_file")
        module.__dict__.clear()  # no entry of a new module (``__name__``, ``__doc__``, ...) is read before the entries of the test
        module.__dict__.update(namespace)
        return mock.patch.object(diagnostic, "loaded_modules", return_value=[module])

    def test_every_inspected_entry_is_charged_so_nested_code_stays_inside_the_limit(self) -> None:
        source = "def f():\n    a = lambda: 1\n    b = lambda: 2\n    c = [x for x in ()]\n    return a, b, c\n"
        scope: dict[str, Any] = {}
        exec(compile(source, OWN_FILE, "exec"), scope)  # noqa: S102
        with self.only({"f": scope["f"]}):
            everything = len(held_code())
            self.assertEqual(3, everything)  # the function and its two lambdas (the comprehension is inline)
            for limit in range(0, everything + 2):
                with self.subTest(limit=limit), mock.patch.object(diagnostic, "REGISTRY_LIMIT", limit):
                    self.assertLessEqual(len(held_code()), limit)

    def test_a_class_namespace_entry_is_charged(self) -> None:
        source = "class C:\n" + "".join(f"    def m{n}(self):\n        return {n}\n" for n in range(6))
        scope: dict[str, Any] = {}
        exec(compile(source, OWN_FILE, "exec"), scope)  # noqa: S102
        with self.only({"C": scope["C"]}):
            self.assertEqual(6, len(held_code()))  # all six methods are found when the limit is not reached
            for limit in range(0, 8):
                with self.subTest(limit=limit), mock.patch.object(diagnostic, "REGISTRY_LIMIT", limit):
                    self.assertLessEqual(len(held_code()), limit)

    def test_a_large_namespace_is_not_copied_before_its_entries_are_counted(self) -> None:
        big = {f"n{n}": n for n in range(300_000)}
        with self.only(big), mock.patch.object(diagnostic, "REGISTRY_LIMIT", 100):
            tracemalloc.start()
            try:
                held_code()
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertLess(peak, 500_000, "the walk copied the namespace before it counted")


if __name__ == "__main__":
    unittest.main()
