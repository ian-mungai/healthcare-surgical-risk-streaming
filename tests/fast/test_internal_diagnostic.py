"""The line that a sealed job writes on ``internal_error`` names a trusted class and the project's own code frames and nothing else.

Failure modes covered here:

- Every sealed job ended an unexpected error as the bare code, so a run of 30 to 80 minutes that ended as ``internal_error`` could not be told from any other,
  and each diagnosis took a patch and a rerun. The line names the class chain and the frames, so the place of the failure is known from the first run.
- The line carries a value. The message, the arguments, the ``repr`` or a local variable can hold a value of the test set, and so can a name: a class that
  a library built from data (its module or its name), a file name, a function compiled from a text (``<string>``) and the frames that PySpark 4 rebuilds from
  the message of a Java error. Only a class on a fixed list (found by identity, so a class with the same name is not on it) is named, and a frame is named
  only when its code object is the very object that a loaded project module holds (``test_internal_diagnostic_rebuilt.py``: PySpark rebuilds frames from a
  message, with any file and function name) and its path (``..`` resolved as text) is exactly the root of the project and a path of the fixed inventory of
  tracked files (``spark/internal_inventory.py``) and its function is a name that the inventory holds for that file (or a fixed name such as ``<lambda>``). A
  generated file name under ``spark/``, a function name that is made at run time beside a tracked file name and a path with ``..`` are all ``other``. Every
  other class and frame is the fixed word ``other``, and no name of one is read.
- Reading a class runs code of a library or of the error: the ``__class__`` of an object, the ``__subclasscheck__`` or ``__instancecheck__`` of a metaclass.
  The lookup uses ``type(error)`` and the ``__mro__`` slot of each class and never ``isinstance`` or ``issubclass`` on a class that it does not own.
- Reading a class or a frame runs code of the error or of a library: a source loader that raises (``traceback`` reads the source lines), a property of an
  error class, a name that raises. The formatter reads no source line and no attribute of an error class, and when anything still fails the line is the
  fixed ``internal_error class=unavailable frames=unavailable``. It never raises, so the exit code and the result write of the job do not change.
- The chain is cut or unbounded: the cause is followed before the context, a cycle ends, and the chain and the frames have a limit (5 classes, 40 frames,
  the innermost frames kept and the innermost last, a run of ``other`` frames collapsed to one).
- A call site that maps an unexpected error to ``internal_error`` writes only the code (the result-file ending, the failed result write, the Silver ending,
  the Bronze lab ending, the reconcile ending and its failed write, the signal profile, the gold dimensions job and the table maintenance job).
"""

from __future__ import annotations

import sys
import types
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from spark import internal_diagnostic as diagnostic
from spark.internal_classes import PACKAGES
from spark.internal_diagnostic import CLASS_LIMIT, FRAME_LIMIT, internal_error_line
from spark.internal_inventory import INVENTORY
from tests.fast.internal_line_support import LINE_MARKER as MARKER_LINE
from tests.fast.internal_line_support import RaisingLoader, assert_internal, error_with_marked_line, stand_in

MARKER = "MARKER_4417"
LINE_MARKER = int(MARKER_LINE)  # a number that a message can carry; it must not be in a line, a file or standard error
ROOT = Path(diagnostic.__file__).resolve().parent.parent
TRACKED = "spark/result_file.py"  # a tracked file of the project (its source is not read: the code is compiled from a text under its name)
ENTRY, INNER = sorted(INVENTORY[TRACKED])[:2]  # two names that the inventory holds for that file
FOREIGN = sorted(set(INVENTORY["spark/profile/checks.py"]) - set(INVENTORY[TRACKED]))[0]  # a name that the inventory holds for another file only
OWN = str(ROOT / TRACKED)


class Secretive(Exception):
    """An error whose every text form holds the marker."""

    def __init__(self) -> None:
        super().__init__(MARKER, {"row": MARKER})
        self.payload = MARKER

    def __str__(self) -> str:
        return MARKER

    def __repr__(self) -> str:
        return f"Secretive({MARKER})"


def innermost(local: str) -> None:
    held = local + MARKER  # noqa: F841 (a local variable that holds the marker)
    raise Secretive


def middle() -> None:
    innermost(MARKER)


def outer() -> None:
    middle()


def caught(raiser: Callable[[], object]) -> BaseException:
    try:
        raiser()
    except BaseException as error:
        return error
    raise AssertionError("no error")


def last_line(error: BaseException) -> int:
    """The line number that the innermost frame of the traceback of ``error`` carries."""
    step = error.__traceback__
    if step is None:
        raise AssertionError("no traceback")
    while step.tb_next is not None:
        step = step.tb_next
    return step.tb_lineno


def nested(depth: int) -> None:
    if depth == 0:
        raise ValueError(MARKER)
    try:
        nested(depth - 1)
    except Exception as error:
        raise (KeyError if depth % 2 else ValueError)(MARKER) from error


def parts(line: str) -> tuple[list[str], list[str]]:
    """The class tokens and the frame tokens of a diagnostic line."""
    head, _, tail = line.partition(" frames=")
    return head.partition("class=")[2].split(" <- "), [frame for frame in tail.split(" | ") if frame]


def from_file(case: unittest.TestCase, filename: str, source: str, entry: str = ENTRY, loader: object | None = None) -> BaseException:
    """The error of ``entry()`` of ``source`` as if it were compiled from the file ``filename`` and held by the loaded module of that file."""
    scope: dict[str, Any] = {"__name__": "fake_job"}
    if loader is not None:
        scope["__loader__"] = loader
    exec(compile(source, filename, "exec"), scope)  # noqa: S102 (a stand-in for the project's own module)
    stand_in(case, filename, scope)
    return caught(scope[entry])


class TheLine(unittest.TestCase):
    def check(self, line: str) -> tuple[list[str], list[str]]:
        """The line has the one shape of the producer, one line and no marker."""
        self.assertTrue(diagnostic.is_diagnostic(line), line)
        self.assertNotIn(MARKER, line)
        self.assertEqual(1, len(line.splitlines()))
        return parts(line)

    def test_a_class_of_the_test_is_other_and_its_frames_are_other_too(self) -> None:
        classes, frames = self.check(internal_error_line(caught(outer)))
        self.assertEqual(["other"], classes)
        self.assertEqual(["other"], frames)  # the frames of this test file are not a package of the project

    def test_the_frames_of_the_project_are_named_with_path_line_and_function(self) -> None:
        source = f"def {ENTRY}():\n    {INNER}()\n\n\ndef {INNER}():\n    raise KeyError\n"
        classes, frames = self.check(internal_error_line(from_file(self, OWN, source)))
        self.assertEqual(["builtins.KeyError"], classes)
        self.assertEqual(["other", f"{TRACKED}:{ENTRY}", f"{TRACKED}:{INNER}"], frames)

    def test_every_package_of_the_project_counts_and_nothing_near_it_does(self) -> None:
        for package in PACKAGES:
            path = next(item for item in sorted(INVENTORY) if item.startswith(f"{package}/") and INVENTORY[item])
            name = INVENTORY[path][0]
            with self.subTest(package=package):
                frames = parts(internal_error_line(from_file(self, str(ROOT / path), f"def {name}():\n    raise KeyError\n", name)))[1]
                self.assertEqual(["other", f"{path}:{name}"], frames)
        source = f"def {ENTRY}():\n    raise KeyError\n"
        for filename in (
            str(ROOT / "tests" / "fast" / "x.py"),  # the tests are not a package of the job
            str(ROOT / "spark_extra" / "x.py"),  # the name begins like a package
            str(ROOT / ".venv" / "lib" / "site-packages" / TRACKED),  # a library that has the name of a package
            str(ROOT.parent / TRACKED),  # a folder beside the project
            TRACKED,  # a relative path is no file of the project
            f"/data/{MARKER}.py",  # a file name of data
            "<string>",
            f"{MARKER}.scala",
        ):
            with self.subTest(filename=filename):
                self.assertEqual(["other"], self.check(internal_error_line(from_file(self, filename, source)))[1])

    def test_a_file_that_the_inventory_does_not_hold_is_other_even_under_a_package_and_with_a_good_name(self) -> None:
        source = f"def {ENTRY}():\n    raise KeyError\n"
        for relative in (
            f"spark/{MARKER}.py",  # a generated file name under spark/
            "spark/generated_by_a_job.py",  # a name of the right form that no tracked file has
            f"spark/{MARKER}-x.py",
            "spark/has space.py",
            "spark/result_file.txt",
            "spark/x.py",
            f"spark/jobs/{MARKER}.py",
            "spark/../tests/fast/x.py",
            f"spark/../{MARKER}.py",
            f"spark/../../{MARKER}.py",
            "spark/../../../../etc/x.py",
            "spark/.hidden/x.py",
        ):
            with self.subTest(relative=relative):
                self.assertEqual(["other"], self.check(internal_error_line(from_file(self, str(ROOT / relative), source)))[1])

    def test_a_path_that_resolves_as_text_to_a_file_of_the_inventory_is_that_file(self) -> None:
        source = f"def {ENTRY}():\n    raise KeyError\n"
        for filename in (f"{ROOT}/spark/../{TRACKED}", f"{ROOT}/./{TRACKED}", f"{ROOT}//{TRACKED}"):
            with self.subTest(filename=filename):
                self.assertEqual(["other", f"{TRACKED}:{ENTRY}"], self.check(internal_error_line(from_file(self, filename, source)))[1])

    def test_a_function_name_that_the_inventory_does_not_hold_for_that_file_is_other(self) -> None:
        for name in (f"{MARKER}_function", FOREIGN, "has space", "a;b", "x" * 101, "é", "", "<string>", "<genexpr> ", ENTRY + "_"):
            with self.subTest(name=name):
                scope: dict[str, Any] = {}
                exec(compile(f"def {ENTRY}():\n    raise KeyError\n", OWN, "exec"), scope)  # noqa: S102
                function = types.FunctionType(
                    scope[ENTRY].__code__.replace(co_name=name), scope
                )  # the name is made at run time, the file name is a tracked one
                stand_in(self, OWN, {"held": function})  # held by the loaded module: only the name is wrong
                self.assertEqual(["other"], self.check(internal_error_line(caught(function)))[1])

    def test_the_fixed_names_of_code_without_a_function_are_named_in_a_file_of_the_inventory(self) -> None:
        source = f"def {ENTRY}():\n    return list(1 / n for n in [0])\n"
        frames = self.check(internal_error_line(from_file(self, OWN, source)))[1]
        self.assertEqual(["other", f"{TRACKED}:{ENTRY}", f"{TRACKED}:<genexpr>"], frames)
        scope: dict[str, Any] = {}
        exec(compile("run = lambda: 1 / 0\n", OWN, "exec"), scope)  # noqa: S102
        stand_in(self, OWN, scope)
        self.assertEqual(["other", f"{TRACKED}:<lambda>"], self.check(internal_error_line(caught(scope["run"])))[1])

    def test_a_frame_is_the_path_and_the_function_and_never_a_line_number(self) -> None:
        self.assertEqual(f"{TRACKED}:{ENTRY}", diagnostic.frame_label(OWN, ENTRY))
        for filename in (OWN.encode(), None, 5):
            with self.subTest(filename=filename):
                self.assertEqual("other", diagnostic.frame_label(filename, ENTRY))
        self.assertEqual("other", diagnostic.frame_label(OWN, None))
        with self.assertRaises(TypeError):
            diagnostic.frame_label(OWN, 5, ENTRY)  # type: ignore[call-arg]  # a function takes no line number

    def test_a_line_number_that_a_message_sets_on_a_tracked_frame_is_not_in_the_line(self) -> None:
        # PySpark 4.1 rebuilds a traceback from the message of a Java error and sets the line of each frame (``co_firstlineno``, ``co_lines``) from a number
        # in it, so a value of the data can be the line number of a frame of a tracked file and function.
        for number in (LINE_MARKER, 1, 63):
            with self.subTest(number=number):
                error = error_with_marked_line(self, str(number))
                self.assertEqual(number, last_line(error))  # the innermost frame carries the number
                line = internal_error_line(error)
                self.assertEqual((["builtins.KeyError"], ["other", f"{TRACKED}:{ENTRY}"]), self.check(line))
                self.assertEqual(f"internal_error class=builtins.KeyError frames=other | {TRACKED}:{ENTRY}", line)
                self.assertNotIn(str(number), line.replace(TRACKED, "").replace(ENTRY, ""))

    def test_a_line_number_that_the_first_line_of_the_code_carries_is_not_in_the_line_either(self) -> None:
        scope: dict[str, Any] = {}
        exec(compile(f"def {ENTRY}():\n    raise KeyError\n", OWN, "exec"), scope)  # noqa: S102
        function = types.FunctionType(scope[ENTRY].__code__.replace(co_firstlineno=LINE_MARKER), scope)
        stand_in(self, OWN, {"held": function})
        line = internal_error_line(caught(function))
        self.assertEqual(f"internal_error class=builtins.KeyError frames=other | {TRACKED}:{ENTRY}", line)
        self.assertNotIn(str(LINE_MARKER + 1), line)
        self.assertNotIn(str(LINE_MARKER), line)

    def test_a_str_that_is_a_subclass_or_is_too_long_is_other(self) -> None:
        class Text(str):
            """A str whose methods could run code."""

        self.assertEqual("other", diagnostic.frame_label(Text(OWN), ENTRY))
        self.assertEqual("other", diagnostic.frame_label(OWN, Text(ENTRY)))
        self.assertEqual("other", diagnostic.frame_label(OWN + "/" * 5000, ENTRY))

    def test_a_function_that_was_compiled_from_a_text_is_other(self) -> None:
        source = f"def {MARKER}():\n    raise KeyError\n"
        scope: dict[str, Any] = {}
        exec(compile(source, "<string>", "exec"), scope)  # noqa: S102
        classes, frames = self.check(internal_error_line(caught(scope[MARKER])))
        self.assertEqual((["builtins.KeyError"], ["other"]), (classes, frames))

    def test_a_run_of_other_frames_is_one_and_the_project_frames_between_stay(self) -> None:
        source = f"def {ENTRY}():\n    callback()\n"
        scope: dict[str, Any] = {"callback": lambda: (_ for _ in ()).throw(KeyError())}
        exec(compile(source, OWN, "exec"), scope)  # noqa: S102
        stand_in(self, OWN, scope)
        _, frames = self.check(internal_error_line(caught(scope[ENTRY])))
        self.assertEqual(["other", f"{TRACKED}:{ENTRY}", "other"], frames)

    def test_the_frames_are_cut_at_forty_with_the_innermost_last(self) -> None:
        source = f"def {ENTRY}(depth=200):\n    if depth == 0:\n        raise RuntimeError\n    {ENTRY}(depth - 1)\n"
        _, frames = self.check(internal_error_line(from_file(self, OWN, source)))
        self.assertEqual(FRAME_LIMIT, len(frames))
        self.assertEqual(f"{TRACKED}:{ENTRY}", frames[-1])
        self.assertNotIn("other", frames)

    def test_an_error_that_was_never_raised_has_a_class_and_no_frames(self) -> None:
        self.assertEqual("internal_error class=builtins.RuntimeError frames=", internal_error_line(RuntimeError(MARKER)))


class TheClasses(unittest.TestCase):
    def classes_of(self, error: BaseException) -> list[str]:
        line = internal_error_line(error)
        self.assertTrue(diagnostic.is_diagnostic(line), line)
        self.assertNotIn(MARKER, line)
        return parts(line)[0]

    def test_a_class_that_a_library_or_a_test_built_from_data_is_other(self) -> None:
        for name, module in ((MARKER, "tests.x"), ("Plain", MARKER), (f"{MARKER}.sub", f"{MARKER}-mod"), ("A-b", "m-n"), ("C" * 500, "m")):
            with self.subTest(name=name, module=module):
                made = type(name, (Exception,), {"__module__": module})
                self.assertEqual(["other"], self.classes_of(made()))

    def test_a_class_with_the_name_and_the_module_of_a_builtin_is_not_that_builtin(self) -> None:
        for base in (Exception, ValueError):
            made = type("ValueError", (base,), {"__module__": "builtins"})
            self.assertEqual(["other"], self.classes_of(made()))
        self.assertEqual(["builtins.ValueError"], self.classes_of(ValueError(MARKER)))
        self.assertEqual(["builtins.KeyboardInterrupt"], self.classes_of(KeyboardInterrupt(MARKER)))
        self.assertEqual(["builtins.ExceptionGroup"], self.classes_of(ExceptionGroup(MARKER, [ValueError()])))

    def test_the_name_and_module_of_a_class_that_is_not_on_the_list_are_never_read(self) -> None:
        read: list[str] = []

        class Spy(type):
            def __getattribute__(cls, name: str) -> Any:
                if name in {"__name__", "__module__", "__qualname__"}:
                    read.append(name)
                return super().__getattribute__(name)

        class Spied(Exception, metaclass=Spy):
            pass

        self.assertEqual(["other"], self.classes_of(Spied()))
        self.assertEqual([], read)

    def test_a_name_that_raises_when_it_is_read_does_not_matter(self) -> None:
        class Raising(type):
            def __getattribute__(cls, name: str) -> Any:
                if name in {"__name__", "__module__", "__qualname__"}:
                    raise RuntimeError(MARKER)
                return super().__getattribute__(name)

        class Hostile(Exception, metaclass=Raising):
            pass

        self.assertEqual(["other"], self.classes_of(Hostile()))

    def test_a_property_of_an_error_class_is_never_run(self) -> None:
        class Hostile(Exception):
            @property
            def __traceback__(self) -> Any:  # type: ignore[override]
                raise RuntimeError(MARKER)

            @property
            def __cause__(self) -> Any:  # type: ignore[override]
                raise RuntimeError(MARKER)

            @property
            def __context__(self) -> Any:  # type: ignore[override]
                raise RuntimeError(MARKER)

        self.assertEqual("internal_error class=other frames=", internal_error_line(Hostile(MARKER)))

    def test_no_hook_of_a_metaclass_or_a_class_attribute_runs_when_the_lists_are_read(self) -> None:
        calls: list[str] = []

        class Hooked(type):
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

        class Trap:
            """An object in the module of a library whose class can be asked for."""

            @property  # type: ignore[misc]
            def __class__(self) -> Any:
                calls.append("class")
                return type

            def __bases__(self) -> Any:
                calls.append("bases")

        errors = types.ModuleType("pyspark.errors")
        base = Hooked("PySparkException", (Exception,), {})
        errors.PySparkException = base  # type: ignore[attr-defined]
        errors.AnalysisException = Hooked("AnalysisException", (base,), {})  # type: ignore[attr-defined]
        errors.ParseException = Trap()  # type: ignore[attr-defined]
        protocol = types.ModuleType("py4j.protocol")
        protocol.Py4JJavaError = Trap()  # type: ignore[attr-defined]
        with mock.patch.dict(sys.modules, {"pyspark.errors": errors, "py4j.protocol": protocol}):
            self.assertEqual(["pyspark.errors.AnalysisException"], self.classes_of(errors.AnalysisException()))
            self.assertEqual(["pyspark.errors.PySparkException"], self.classes_of(base()))
            self.assertEqual(["builtins.ValueError"], self.classes_of(ValueError(MARKER)))
            self.assertEqual(["other"], self.classes_of(Hooked(MARKER, (Exception,), {})(MARKER)))  # a class of data with the same hooks
        self.assertEqual([], calls)

    def test_a_class_that_claims_another_class_is_the_class_that_it_is(self) -> None:
        class Liar(Exception):
            @property  # type: ignore[misc]
            def __class__(self) -> Any:
                return ValueError

        self.assertEqual(["other"], self.classes_of(Liar(MARKER)))

    def test_a_class_of_the_project_is_named_by_its_module(self) -> None:
        from scripts.stack.nessie import NessieError
        from spark.settings import SettingsError

        self.assertEqual(["scripts.stack.nessie.NessieError"], self.classes_of(NessieError("x", status=500)))
        self.assertEqual(["spark.settings.SettingsError"], self.classes_of(SettingsError("x")))

        class Mine(SettingsError):
            pass

        self.assertEqual(["other"], self.classes_of(Mine("x")))  # a subclass is another class: only the listed classes are named

    def test_a_class_of_a_library_is_named_only_when_it_is_the_class_of_the_library(self) -> None:
        errors = types.ModuleType("pyspark.errors")
        base = type("PySparkException", (Exception,), {})
        errors.PySparkException = base  # type: ignore[attr-defined]
        errors.AnalysisException = type("AnalysisException", (base,), {})  # type: ignore[attr-defined]
        errors.NotAnError = object()  # type: ignore[attr-defined]
        protocol = types.ModuleType("py4j.protocol")
        protocol.Py4JJavaError = type("Py4JJavaError", (Exception,), {})  # type: ignore[attr-defined]
        protocol.Py4JNetworkError = type("Py4JNetworkError", (Exception,), {})  # type: ignore[attr-defined]
        lookalike = type("AnalysisException", (base,), {"__module__": "pyspark.errors"})
        unrelated = type("ParseException", (Exception,), {"__module__": "pyspark.errors"})
        errors.ParseException = unrelated  # type: ignore[attr-defined]  # on the list by name, but not a PySparkException
        self.assertEqual(["other"], self.classes_of(errors.AnalysisException()))  # the module is not loaded (nothing is imported)
        with mock.patch.dict(sys.modules, {"pyspark.errors": errors, "py4j.protocol": protocol}):
            self.assertEqual(["pyspark.errors.PySparkException"], self.classes_of(base()))
            self.assertEqual(["pyspark.errors.AnalysisException"], self.classes_of(errors.AnalysisException()))
            self.assertEqual(["py4j.protocol.Py4JJavaError"], self.classes_of(protocol.Py4JJavaError(MARKER)))
            self.assertEqual(["py4j.protocol.Py4JNetworkError"], self.classes_of(protocol.Py4JNetworkError()))
            self.assertEqual(["other"], self.classes_of(lookalike()))
            self.assertEqual(["other"], self.classes_of(unrelated()))

    def test_the_standard_library_classes_that_the_jobs_can_meet_are_named(self) -> None:
        import json
        import socket
        import subprocess

        self.assertEqual(["json.decoder.JSONDecodeError"], self.classes_of(json.JSONDecodeError("m", "d", 0)))
        self.assertEqual(["socket.gaierror"], self.classes_of(socket.gaierror(1, "x")))
        self.assertEqual(["subprocess.TimeoutExpired"], self.classes_of(subprocess.TimeoutExpired("x", 1)))

    def test_a_cause_is_followed_before_a_context_and_the_top_error_comes_first(self) -> None:
        def raiser() -> None:
            try:
                try:
                    raise ValueError(MARKER)
                except ValueError:
                    raise KeyError(MARKER) from OSError(MARKER)  # the cause wins over the context (the ValueError)
            except KeyError:
                raise RuntimeError(MARKER)  # noqa: B904 (the implicit context is the KeyError)

        self.assertEqual(["builtins.RuntimeError", "builtins.KeyError", "builtins.OSError"], self.classes_of(caught(raiser)))

    def test_an_error_of_another_class_inside_the_chain_is_other_at_its_place(self) -> None:
        first = type(MARKER, (Exception,), {})(MARKER)
        top = RuntimeError(MARKER)
        top.__cause__ = first
        self.assertEqual(["builtins.RuntimeError", "other"], self.classes_of(top))

    def test_a_chain_is_cut_at_five_classes(self) -> None:
        classes = self.classes_of(caught(lambda: nested(10)))
        self.assertEqual(CLASS_LIMIT, len(classes))
        self.assertEqual(["builtins.ValueError", "builtins.KeyError", "builtins.ValueError", "builtins.KeyError", "builtins.ValueError"], classes)

    def test_a_chain_that_loops_ends(self) -> None:
        first, second = ValueError(MARKER), KeyError(MARKER)
        first.__context__, second.__context__ = second, first
        self.assertEqual("internal_error class=builtins.ValueError <- builtins.KeyError frames=", internal_error_line(first))


class TheFallback(unittest.TestCase):
    def test_a_source_loader_that_raises_does_not_stop_the_line(self) -> None:
        source = f"def {ENTRY}():\n    raise KeyError\n"
        line = internal_error_line(from_file(self, OWN, source, loader=RaisingLoader()))
        self.assertEqual(f"internal_error class=builtins.KeyError frames=other | {TRACKED}:{ENTRY}", line)

    def test_no_source_line_of_any_file_is_read(self) -> None:
        with mock.patch("linecache.getlines", side_effect=AssertionError("a source line was read")) as read, mock.patch("linecache.getline", read):
            self.assertTrue(diagnostic.is_diagnostic(internal_error_line(from_file(self, OWN, f"def {ENTRY}():\n    raise KeyError\n"))))

    def test_any_failure_of_the_formatter_is_the_fixed_line_and_never_an_error(self) -> None:
        self.assertEqual("internal_error class=unavailable frames=unavailable", diagnostic.UNAVAILABLE_LINE)
        self.assertTrue(diagnostic.is_diagnostic(diagnostic.UNAVAILABLE_LINE))
        for function in ("class_chain", "frames_of"):
            with self.subTest(function=function), mock.patch.object(diagnostic, function, side_effect=RuntimeError(MARKER)):
                self.assertEqual(diagnostic.UNAVAILABLE_LINE, internal_error_line(caught(outer)))

    def test_a_traceback_that_cannot_be_walked_is_the_fixed_line(self) -> None:
        class Cycle:
            """A traceback that never ends (the real type cannot be built that way)."""

            tb_frame = types.SimpleNamespace(f_code=types.SimpleNamespace(co_filename="x", co_name="f"))

            @property
            def tb_next(self) -> Any:
                return self

        with mock.patch.object(diagnostic, "traceback_of", return_value=Cycle()):
            self.assertEqual(diagnostic.UNAVAILABLE_LINE, internal_error_line(caught(outer)))

    def test_a_frame_whose_code_raises_when_it_is_read_is_the_fixed_line(self) -> None:
        class Frame:
            @property
            def f_code(self) -> Any:
                raise RuntimeError(MARKER)

        walk = types.SimpleNamespace(tb_frame=Frame(), tb_next=None)
        with mock.patch.object(diagnostic, "traceback_of", return_value=walk):
            self.assertEqual(diagnostic.UNAVAILABLE_LINE, internal_error_line(caught(outer)))


class TheHelperCheck(unittest.TestCase):
    def test_the_check_of_the_tests_catches_a_message(self) -> None:
        with self.assertRaises(AssertionError):
            assert_internal(self, f"job: internal_error class=builtins.RuntimeError frames=\n{MARKER}\n", "job", None, ("builtins.RuntimeError",), MARKER)

    def test_the_check_of_the_tests_catches_a_class_that_is_not_on_the_list(self) -> None:
        with self.assertRaises(AssertionError):
            assert_internal(self, f"job: internal_error class=tests.x.{MARKER} frames=\n", "job", None, (), MARKER)


if __name__ == "__main__":
    unittest.main()
