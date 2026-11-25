"""The line that a sealed job writes on ``internal_error`` names a trusted class and the project's own code frames and nothing else.

Failure modes covered here:

- Every sealed job ended an unexpected error as the bare code, so a run of 30 to 80 minutes that ended as ``internal_error`` could not be told from any other,
  and each diagnosis took a patch and a rerun. The line names the class chain and the frames, so the place of the failure is known from the first run.
- The line carries a value. The message, the arguments, the ``repr`` or a local variable can hold a value of the test set, and so can a name: a class that
  a library built from data (its module or its name), a file name, a function compiled from a text (``<string>``) and the frames that PySpark 4 rebuilds from
  the message of a Java error. Only a class on a fixed list (found by identity, so a class with the same name is not on it) is named, and a frame is named
  only when its file is a file of the project's own packages. Every other class and frame is the fixed word ``other``, and no name of one is read.
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
from tests.fast.internal_line_support import RaisingLoader, assert_internal

MARKER = "MARKER_4417"
ROOT = Path(diagnostic.__file__).parent.parent
OWN = str(ROOT / "spark" / "fake_job.py")  # a file under a package of the project (the file does not exist: a source line cannot be read)


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


def from_file(filename: str, source: str, entry: str = "entry", loader: object | None = None) -> BaseException:
    """The error of ``entry()`` of ``source`` as if it were compiled from the file ``filename``."""
    scope: dict[str, Any] = {"__name__": "fake_job"}
    if loader is not None:
        scope["__loader__"] = loader
    exec(compile(source, filename, "exec"), scope)  # noqa: S102 (a stand-in for the project's own module)
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

    def test_the_frames_of_the_project_are_named_with_file_line_and_function(self) -> None:
        source = "def entry():\n    inner()\n\n\ndef inner():\n    raise KeyError\n"
        classes, frames = self.check(internal_error_line(from_file(OWN, source)))
        self.assertEqual(["builtins.KeyError"], classes)
        self.assertEqual(["other", "fake_job.py:2:entry", "fake_job.py:6:inner"], frames)

    def test_every_package_of_the_project_counts_and_nothing_near_it_does(self) -> None:
        source = "def entry():\n    raise KeyError\n"
        for package in PACKAGES:
            with self.subTest(package=package):
                frames = parts(internal_error_line(from_file(str(ROOT / package / "x.py"), source)))[1]
                self.assertEqual(["other", "x.py:2:entry"], frames)
        for filename in (
            str(ROOT / "tests" / "fast" / "x.py"),  # the tests are not a package of the job
            str(ROOT / "spark_extra" / "x.py"),  # the name begins like a package
            str(ROOT / ".venv" / "lib" / "site-packages" / "spark" / "x.py"),  # a library that has the name of a package
            str(ROOT.parent / "spark" / "x.py"),  # a folder beside the project
            "spark/x.py",  # a relative path is no file of the project
            f"/data/{MARKER}.py",  # a file name of data
            "<string>",
            f"{MARKER}.scala",
        ):
            with self.subTest(filename=filename):
                self.assertEqual(["other"], self.check(internal_error_line(from_file(filename, source)))[1])

    def test_a_file_name_or_function_name_that_is_not_a_name_of_the_project_is_other(self) -> None:
        for filename in (str(ROOT / "spark" / f"{MARKER}-x.py"), str(ROOT / "spark" / "has space.py"), str(ROOT / "spark" / "x.txt")):
            with self.subTest(filename=filename):
                self.assertEqual(["other"], self.check(internal_error_line(from_file(filename, "def entry():\n    raise KeyError\n")))[1])
        for name in (f"{MARKER}-x", "has space", "a;b", "x" * 101, "é"):
            with self.subTest(name=name):
                code = compile("def entry():\n    raise KeyError\n", OWN, "exec")
                scope: dict[str, Any] = {}
                exec(code, scope)  # noqa: S102
                function = types.FunctionType(scope["entry"].__code__.replace(co_name=name), scope)
                self.assertEqual(["other"], self.check(internal_error_line(caught(function)))[1])

    def test_a_function_that_was_compiled_from_a_text_is_other(self) -> None:
        source = f"def {MARKER}():\n    raise KeyError\n"
        scope: dict[str, Any] = {}
        exec(compile(source, "<string>", "exec"), scope)  # noqa: S102
        classes, frames = self.check(internal_error_line(caught(scope[MARKER])))
        self.assertEqual((["builtins.KeyError"], ["other"]), (classes, frames))

    def test_the_frames_that_pyspark_builds_from_the_message_of_a_java_error_are_other(self) -> None:
        # PySpark 4 compiles one frame for each line of the Java stack, with the file and the function of the line (a name that can come from the message).
        code = compile("\n" * 8 + "raise ValueError('x')", f"{MARKER}.scala", "exec").replace(co_name=f"{MARKER}_function")
        classes, frames = self.check(internal_error_line(caught(lambda: exec(code, {}))))  # noqa: S102
        self.assertEqual((["builtins.ValueError"], ["other"]), (classes, frames))

    def test_a_run_of_other_frames_is_one_and_the_project_frames_between_stay(self) -> None:
        source = "def entry():\n    callback()\n"
        scope: dict[str, Any] = {"callback": lambda: (_ for _ in ()).throw(KeyError())}
        exec(compile(source, OWN, "exec"), scope)  # noqa: S102
        _, frames = self.check(internal_error_line(caught(scope["entry"])))
        self.assertEqual(["other", "fake_job.py:2:entry", "other"], frames)

    def test_the_frames_are_cut_at_forty_with_the_innermost_last(self) -> None:
        source = "def entry(depth=200):\n    if depth == 0:\n        raise RuntimeError\n    entry(depth - 1)\n"
        _, frames = self.check(internal_error_line(from_file(OWN, source)))
        self.assertEqual(FRAME_LIMIT, len(frames))
        self.assertEqual("fake_job.py:3:entry", frames[-1])
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
        source = "def entry():\n    raise KeyError\n"
        line = internal_error_line(from_file(OWN, source, loader=RaisingLoader()))
        self.assertEqual("internal_error class=builtins.KeyError frames=other | fake_job.py:2:entry", line)

    def test_no_source_line_of_any_file_is_read(self) -> None:
        with mock.patch("linecache.getlines", side_effect=AssertionError("a source line was read")) as read, mock.patch("linecache.getline", read):
            self.assertTrue(diagnostic.is_diagnostic(internal_error_line(from_file(OWN, "def entry():\n    raise KeyError\n"))))

    def test_any_failure_of_the_formatter_is_the_fixed_line_and_never_an_error(self) -> None:
        self.assertEqual("internal_error class=unavailable frames=unavailable", diagnostic.UNAVAILABLE_LINE)
        self.assertTrue(diagnostic.is_diagnostic(diagnostic.UNAVAILABLE_LINE))
        for function in ("class_chain", "frames_of"):
            with self.subTest(function=function), mock.patch.object(diagnostic, function, side_effect=RuntimeError(MARKER)):
                self.assertEqual(diagnostic.UNAVAILABLE_LINE, internal_error_line(caught(outer)))

    def test_a_traceback_that_cannot_be_walked_is_the_fixed_line(self) -> None:
        class Cycle:
            """A traceback that never ends (the real type cannot be built that way)."""

            tb_lineno = 1
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

        walk = types.SimpleNamespace(tb_frame=Frame(), tb_next=None, tb_lineno=1)
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
