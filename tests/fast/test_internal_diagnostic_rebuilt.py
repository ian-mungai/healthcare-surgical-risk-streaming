"""A frame that PySpark rebuilds from the text of an error is never named in the line that a sealed job writes on ``internal_error``.

PySpark 4.1.4 (``recover_python_exception``, ``pyspark/errors/exceptions/base.py`` lines 470-491, with ``Traceback.as_traceback``, ``tblib.py`` lines 184-205)
reads ``File "<name>", line <n>, in <function>`` lines from the message of a Java error and makes a NEW code object for each one with that file name, that
function name and that line number (``stub.replace(co_filename=..., co_name=..., co_firstlineno=...)``). So a message could choose the label that the line
prints: any path and function of the fixed inventory (``spark/internal_inventory.py``) passes a check of names alone. The tests drive the recovery logic of
PySpark itself (a vendored copy of the pinned source, ``tests/fast/pyspark_recovery.py``, because PySpark is not installed here).

Failure modes covered here:

- A rebuilt frame names a real file and function of the inventory (and a fixed name, and a path with ``..``): it is ``other``. A frame is named only when its
  code object is the very object that a loaded project module holds (found by ``is``, never by name), and a rebuilt code object is always a new object.
- A copy of real code (``replace``), the same source compiled again under the same file name, a module that is not loaded and a module-like object in
  ``sys.modules`` are not the code that a loaded project module holds: ``other``.
- A real frame next to a rebuilt one stays named, and every kind of definition that a project module holds is found: a function, a method, a static method, a
  class method, a property, a nested function, a lambda, a generator expression, a cached function, a decorated function (through its closure) and a function
  of the main module.
- Walking a module runs no code of an object in it (a ``__class__`` property, a metaclass hook), and a walk that is too long ends with the frames that it has
  named so far: the line is still a line of the grammar, never the unavailable line.
"""

from __future__ import annotations

import sys
import types
import unittest
from typing import Any
from unittest import mock

from spark import internal_diagnostic as diagnostic
from spark import result_file
from spark.internal_classes import PACKAGES
from spark.internal_diagnostic import FIXED_NAMES, internal_error_line
from spark.internal_inventory import INVENTORY
from tests.fast.internal_line_support import LINE_MARKER, OWN_FILE, ROOT, TRACKED, stand_in
from tests.fast.pyspark_recovery import message_naming, recover_python_exception

MARKER = "REBUILT_5532"
NUMBER = int(LINE_MARKER)
NAMES = [name for name in sorted(INVENTORY[TRACKED]) if name.isidentifier() and not name.startswith("_")]
REAL = next(name for name in NAMES if type(vars(result_file).get(name)) is types.FunctionType)  # a function that the file really defines
ONLY_OTHER = "internal_error class=builtins.RuntimeError frames=other"


def rebuilt(filename: str, name: str, number: int = NUMBER) -> RuntimeError:
    """An error whose traceback PySpark recovers from its message, which names ``filename`` and ``name`` and the line ``number`` and holds the marker."""
    error = recover_python_exception(RuntimeError(message_naming(filename, name, number, MARKER)))
    step = error.__traceback__
    if step is None or step.tb_next is not None:
        raise AssertionError("the recovery made no frame")
    if (step.tb_frame.f_code.co_filename, step.tb_frame.f_code.co_name, step.tb_lineno) != (filename, name, number):
        raise AssertionError("the frame is not as the text says")
    return error


def caught(raiser: Any) -> BaseException:
    try:
        raiser()
    except BaseException as error:
        return error
    raise AssertionError("no error")


def frames_of(line: str) -> list[str]:
    if not diagnostic.is_diagnostic(line) or MARKER in line or LINE_MARKER in line:
        raise AssertionError(line)
    return [frame for frame in line.partition(" frames=")[2].split(" | ") if frame]


class TheRebuiltFrames(unittest.TestCase):
    def test_a_rebuilt_frame_that_names_a_real_function_of_a_real_file_is_other(self) -> None:
        real = vars(result_file)[REAL].__code__
        error = rebuilt(OWN_FILE, REAL)
        step = error.__traceback__
        if step is None:
            raise AssertionError("no traceback")
        self.assertIsNot(real, step.tb_frame.f_code)  # the same file name and function name, and a new object
        self.assertEqual((real.co_filename, real.co_name), (step.tb_frame.f_code.co_filename, step.tb_frame.f_code.co_name))
        self.assertEqual(ONLY_OTHER, internal_error_line(error))

    def test_a_rebuilt_frame_is_other_for_every_package_and_every_fixed_name_and_a_path_with_dots(self) -> None:
        cases = []
        for package in PACKAGES:
            path = next(item for item in sorted(INVENTORY) if item.startswith(f"{package}/") and INVENTORY[item])
            cases.append((str(ROOT / path), INVENTORY[path][0]))
        cases += [(OWN_FILE, name) for name in FIXED_NAMES]
        cases += [(f"{MARKER}.scala", f"{MARKER}_function"), ("<string>", REAL), (f"/data/{MARKER}.py", REAL)]  # the frames of a Java stack and of a data file
        cases += [(f"{ROOT}/spark/../{TRACKED}", REAL), (f"{ROOT}/./{TRACKED}", REAL), (f"{ROOT}//{TRACKED}", REAL)]
        for filename, name in cases:
            with self.subTest(filename=filename, name=name):
                self.assertEqual(ONLY_OTHER, internal_error_line(rebuilt(filename, name)))

    def test_a_rebuilt_frame_between_real_ones_is_other_and_the_real_ones_stay_named(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(f"def {REAL}(error):\n    raise error\n", OWN_FILE, "exec"), scope)  # noqa: S102 (a stand-in for a function of the project)
        stand_in(self, OWN_FILE, scope)
        error = rebuilt(OWN_FILE, REAL)
        self.assertEqual(["other", f"{TRACKED}:{REAL}", "other"], frames_of(internal_error_line(caught(lambda: scope[REAL](error)))))

    def test_the_line_number_and_the_marker_of_the_message_are_nowhere_in_the_line(self) -> None:
        line = internal_error_line(rebuilt(OWN_FILE, REAL))
        for text in (MARKER, str(NUMBER), "Traceback", "JVM"):
            self.assertNotIn(text, line)


class TheIdentity(unittest.TestCase):
    SOURCE = f"def {REAL}():\n    raise KeyError\n"

    def run_it(self, scope: dict[str, Any]) -> str:
        return internal_error_line(caught(scope[REAL]))

    def test_the_code_that_a_loaded_module_holds_is_named(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102
        stand_in(self, OWN_FILE, scope)
        self.assertEqual(["other", f"{TRACKED}:{REAL}"], frames_of(self.run_it(scope)))

    def test_the_same_source_compiled_again_that_no_module_holds_is_other(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102
        held: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), held)  # noqa: S102
        stand_in(self, OWN_FILE, held)  # a module holds one copy of the code, and the frame ran another
        self.assertEqual(["other"], frames_of(self.run_it(scope)))

    def test_a_copy_of_real_code_is_other(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102
        stand_in(self, OWN_FILE, scope)
        code = scope[REAL].__code__
        for copy in (code.replace(co_firstlineno=NUMBER), code.replace(co_name=REAL), code.replace()):
            with self.subTest(copy=copy.co_firstlineno):
                function = types.FunctionType(copy, {})
                self.assertEqual(["other"], frames_of(internal_error_line(caught(function))))

    def test_code_of_a_module_that_is_not_loaded_is_other(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102
        self.assertEqual(["other"], frames_of(self.run_it(scope)))

    def test_an_entry_of_sys_modules_that_is_not_a_module_is_ignored(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102

        class Imitation(types.ModuleType):
            pass

        name = "spark.result_file"
        for entry in (types.SimpleNamespace(**scope), Imitation(name)):
            entry.__dict__.update({"held": scope[REAL]})
            with self.subTest(kind=type(entry).__name__), mock.patch.dict(sys.modules, {name: entry}):
                self.assertEqual(["other"], frames_of(self.run_it(scope)))

    def test_a_function_of_the_main_module_is_named(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(self.SOURCE, OWN_FILE, "exec"), scope)  # noqa: S102
        self.assertEqual(["other"], frames_of(self.run_it(scope)))
        with mock.patch.dict(sys.modules["__main__"].__dict__, {"_stand_in": scope[REAL]}):
            self.assertEqual(["other", f"{TRACKED}:{REAL}"], frames_of(self.run_it(scope)))

    def test_every_kind_of_definition_that_a_module_holds_is_found(self) -> None:
        function, cls, method, static, classmethod_, prop, outer, inner, cached, decorate, wrapper, decorated = NAMES[:12]
        source = f"""
import functools

def {function}():
    raise KeyError

class {cls}:
    def {method}(self):
        raise KeyError

    @staticmethod
    def {static}():
        raise KeyError

    @classmethod
    def {classmethod_}(cls):
        raise KeyError

    @property
    def {prop}(self):
        raise KeyError

def {outer}():
    def {inner}():
        raise KeyError
    {inner}()

@functools.cache
def {cached}():
    raise KeyError

def {decorate}(function):
    @functools.wraps(function)
    def {wrapper}():
        return function()
    return {wrapper}

@{decorate}
def {decorated}():
    raise KeyError

lambdas = lambda: 1 / 0
generated = lambda: list(1 / n for n in [0])
"""
        scope: dict[str, Any] = {}
        exec(compile(source, OWN_FILE, "exec"), scope)  # noqa: S102
        stand_in(self, OWN_FILE, scope)
        instance = scope[cls]()
        cases = {
            function: (scope[function], [function]),
            method: (getattr(instance, method), [method]),
            static: (scope[cls].__dict__[static].__func__, [static]),
            classmethod_: (scope[cls].__dict__[classmethod_].__func__.__get__(scope[cls]), [classmethod_]),
            prop: (lambda: scope[cls].__dict__[prop].fget(instance), [prop]),
            outer: (scope[outer], [outer, inner]),
            cached: (scope[cached], [cached]),
            decorated: (scope[decorated], [wrapper, decorated]),
            "lambda": (scope["lambdas"], ["<lambda>"]),
            "genexpr": (scope["generated"], ["<lambda>", "<genexpr>"]),
        }
        for label, (raiser, names) in cases.items():
            with self.subTest(kind=label):
                tail = [f"{TRACKED}:{name}" for name in names]
                found = frames_of(internal_error_line(caught(raiser)))
                self.assertEqual(tail, [frame for frame in found if frame != "other"][-len(tail) :], found)
                self.assertNotIn("other", found[len(found) - len(tail) :])


class TheWalk(unittest.TestCase):
    def named_error(self) -> BaseException:
        scope: dict[str, Any] = {}
        exec(compile(f"def {REAL}():\n    raise KeyError\n", OWN_FILE, "exec"), scope)  # noqa: S102
        stand_in(self, OWN_FILE, scope)
        return caught(scope[REAL])

    def test_no_code_of_an_object_in_a_module_runs_while_the_module_is_walked(self) -> None:
        calls: list[str] = []

        class Hooked(type):
            def __getattribute__(cls, name: str) -> Any:
                calls.append(f"getattribute:{name}")
                return super().__getattribute__(name)

            def __subclasscheck__(cls, other: Any) -> bool:
                calls.append("subclasscheck")
                return True

            def __instancecheck__(cls, other: Any) -> bool:
                calls.append("instancecheck")
                return True

            def __hash__(cls) -> int:
                calls.append("hash")
                return 1

            def __eq__(cls, other: object) -> bool:
                calls.append("eq")
                return True

            def __iter__(cls) -> Any:
                calls.append("iter")
                return iter(())

        class Trap:
            @property  # type: ignore[misc]
            def __class__(self) -> Any:
                calls.append("class")
                return type

            def __getattr__(self, name: str) -> Any:
                calls.append(f"getattr:{name}")
                raise RuntimeError(MARKER)

            def __getitem__(self, key: Any) -> Any:
                calls.append("getitem")
                raise RuntimeError(MARKER)

        Held = Hooked("Held", (), {"method": lambda self: 1, "__module__": "x"})  # noqa: N806
        module = vars(sys.modules["spark.result_file"])
        with mock.patch.dict(module, {"_trap": Trap(), "_Held": Held, "_held": Held()}):
            error = self.named_error()
            calls.clear()
            line = internal_error_line(error)
        self.assertEqual([], calls)
        self.assertEqual(["other", f"{TRACKED}:{REAL}"], frames_of(line))

    def test_a_walk_that_is_too_long_names_nothing_more_and_is_still_a_line_of_the_grammar(self) -> None:
        error = self.named_error()
        with mock.patch.object(diagnostic, "REGISTRY_LIMIT", 1):
            line = internal_error_line(error)
        self.assertEqual("internal_error class=builtins.KeyError frames=other", line)
        self.assertTrue(diagnostic.is_diagnostic(line))
        self.assertEqual(["other", f"{TRACKED}:{REAL}"], frames_of(internal_error_line(error)))


if __name__ == "__main__":
    unittest.main()
