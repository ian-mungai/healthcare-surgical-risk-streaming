"""The host mode of the Silver acceptance runners reads its settings exactly, has no default path on the host and keeps every wait inside a stated figure.

The shared helper is ``tests/fast/host_mode.py``. The Silver runners (untracked) call it, so these tests run everywhere and need no Docker.

Failure modes covered here:

- A host flag with a value other than ``1`` (``true``, ``yes``, `` 1``) ran the run in local mode on the host, with the local defaults and the local limits,
  and nothing said so.
- A folder of the pinned files fell back to a path of the lead's computer on the host, or an unset or relative folder was used as it stood.
- ``SPARK_IMAGE`` that is set to an empty text counted as "an image was given", so the run neither built the image nor had one, and a tag that the run did not
  build was left or removed. A tag that starts with a dash or holds a space reached ``docker`` as an option.
- The host run started with a daemon that has the memory in total but not free (the deployed stack holds the rest), or with no free-memory figure at all.
- A step limit on the host was longer than the local one (a stalled step then waits longer on the billed host), a job's own deadline was not shorter than the
  runner's outer limit (the runner then kills a job that is still inside its own deadline, and the job's exit 124 is never read), or the run had no
  deadline of its own.
"""

from __future__ import annotations

import dataclasses
import unittest
from pathlib import Path

from tests.fast import host_mode
from tests.fast.host_mode import (
    HOST_LIMITS,
    LOCAL_LIMITS,
    HostSettingError,
    host_flag,
    image_choice,
    limits_for,
    memory_problem,
    pinned_folders,
    run_expired,
)

LANDED_DEFAULT, EXTRA_DEFAULT = Path("/default/landed"), Path("/default/extra")
GIB = 1024**3


class HostFlagTest(unittest.TestCase):
    def test_only_one_turns_the_host_mode_on_and_an_unset_or_zero_flag_is_the_local_mode(self) -> None:
        self.assertIs(True, host_flag({"RUN_HOST": "1"}, "RUN_HOST"))
        for environ in ({}, {"RUN_HOST": ""}, {"RUN_HOST": "0"}):
            with self.subTest(environ=environ):
                self.assertIs(False, host_flag(environ, "RUN_HOST"))

    def test_every_other_value_is_refused_by_the_name_of_the_setting_and_never_read_as_local(self) -> None:
        for value in ("true", "yes", " 1", "1 ", "01", "2", "on"):
            with self.subTest(value=value), self.assertRaises(HostSettingError) as caught:
                host_flag({"RUN_HOST": value}, "RUN_HOST")
            self.assertEqual("RUN_HOST must be 1, 0 or not set", str(caught.exception))


class PinnedFoldersTest(unittest.TestCase):
    def folders(self, environ: dict[str, str], host: bool) -> host_mode.Folders:
        return pinned_folders(environ, host, LANDED_DEFAULT, EXTRA_DEFAULT)

    def test_a_local_run_without_settings_has_the_defaults_and_no_problem(self) -> None:
        result = self.folders({}, False)
        self.assertEqual((LANDED_DEFAULT, EXTRA_DEFAULT, ()), (result.landed, result.extra, result.problems))

    def test_a_local_run_takes_the_folders_that_it_is_given(self) -> None:
        result = self.folders({"LANDED_CACHE": "/a", "SIGNAL_CASE_CACHE": "/b"}, False)
        self.assertEqual((Path("/a"), Path("/b"), ()), (result.landed, result.extra, result.problems))

    def test_a_host_run_has_no_default_path_and_names_each_missing_setting(self) -> None:
        result = self.folders({}, True)
        self.assertEqual((None, None), (result.landed, result.extra))
        self.assertEqual(("LANDED_CACHE is not set",), result.problems)

    def test_a_host_run_with_only_the_case_folder_still_misses_the_landed_folder_and_keeps_no_default(self) -> None:
        result = self.folders({"SIGNAL_CASE_CACHE": "/b"}, True)
        self.assertEqual(("LANDED_CACHE is not set",), result.problems)
        self.assertIsNone(result.landed)

    def test_the_case_folder_falls_back_to_the_landed_folder_on_the_host(self) -> None:
        result = self.folders({"LANDED_CACHE": "/a"}, True)
        self.assertEqual((Path("/a"), Path("/a"), ()), (result.landed, result.extra, result.problems))

    def test_both_folders_on_the_host_are_used_as_given(self) -> None:
        result = self.folders({"LANDED_CACHE": "/a", "SIGNAL_CASE_CACHE": "/b"}, True)
        self.assertEqual((Path("/a"), Path("/b"), ()), (result.landed, result.extra, result.problems))

    def test_an_empty_setting_counts_as_not_set_in_both_modes(self) -> None:
        self.assertEqual(("LANDED_CACHE is not set",), self.folders({"LANDED_CACHE": ""}, True).problems)
        result = self.folders({"LANDED_CACHE": "", "SIGNAL_CASE_CACHE": ""}, False)
        self.assertEqual((LANDED_DEFAULT, EXTRA_DEFAULT), (result.landed, result.extra))

    def test_a_relative_folder_on_the_host_is_refused_because_it_would_follow_the_working_folder(self) -> None:
        result = self.folders({"LANDED_CACHE": "cache", "SIGNAL_CASE_CACHE": "/b"}, True)
        self.assertEqual(("LANDED_CACHE is not an absolute path",), result.problems)
        result = self.folders({"LANDED_CACHE": "/a", "SIGNAL_CASE_CACHE": "cases"}, True)
        self.assertEqual(("SIGNAL_CASE_CACHE is not an absolute path",), result.problems)

    def test_a_problem_names_the_setting_and_never_its_value(self) -> None:
        result = self.folders({"LANDED_CACHE": "relative/secret_name"}, True)
        self.assertNotIn("secret_name", " ".join(result.problems))


class ImageChoiceTest(unittest.TestCase):
    TAG = "surgical-risk-spark:p6-sig"

    def test_without_a_given_image_the_run_builds_its_own_tag_and_removes_it(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, True), image_choice({}, self.TAG))

    def test_an_empty_image_setting_is_not_a_given_image(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, True), image_choice({"SPARK_IMAGE": ""}, self.TAG))

    def test_a_given_image_is_used_and_left_as_it_is(self) -> None:
        self.assertEqual(host_mode.Image("surgical-risk-spark:4.1.4", False), image_choice({"SPARK_IMAGE": "surgical-risk-spark:4.1.4"}, self.TAG))

    def test_a_reference_that_docker_could_read_as_an_option_or_as_two_words_is_refused(self) -> None:
        for value in ("--rm", "-v", "a b", "image;rm", "image:", "UPPER:tag", "a:b:c", "image\n", "/abs/path"):
            with self.subTest(value=value), self.assertRaises(HostSettingError) as caught:
                image_choice({"SPARK_IMAGE": value}, self.TAG)
            self.assertEqual("SPARK_IMAGE is not an image name with a tag", str(caught.exception))

    def test_a_registry_name_with_a_port_and_a_tag_is_accepted(self) -> None:
        self.assertEqual("registry.local:5000/team/spark:1.2", image_choice({"SPARK_IMAGE": "registry.local:5000/team/spark:1.2"}, self.TAG).tag)

    def test_a_name_without_a_tag_is_refused_because_it_would_follow_latest(self) -> None:
        with self.assertRaises(HostSettingError):
            image_choice({"SPARK_IMAGE": "surgical-risk-spark"}, self.TAG)


class MemoryTest(unittest.TestCase):
    NEEDED = 6 * GIB

    def test_a_local_run_needs_twice_the_needed_memory_in_the_daemon_and_no_free_figure(self) -> None:
        self.assertIsNone(memory_problem(12 * GIB, None, self.NEEDED, False))
        self.assertEqual("daemon_memory", memory_problem(12 * GIB - 1, None, self.NEEDED, False))

    def test_a_host_run_needs_the_daemon_memory_and_the_needed_memory_free(self) -> None:
        self.assertIsNone(memory_problem(32 * GIB, 6 * 1024, self.NEEDED, True))
        self.assertEqual("available_memory", memory_problem(32 * GIB, 6 * 1024 - 1, self.NEEDED, True))

    def test_a_host_run_with_no_free_memory_figure_is_refused_never_assumed_to_fit(self) -> None:
        self.assertEqual("available_unknown", memory_problem(32 * GIB, None, self.NEEDED, True))

    def test_the_daemon_total_is_checked_first_on_the_host(self) -> None:
        self.assertEqual("daemon_memory", memory_problem(8 * GIB, 100 * 1024, self.NEEDED, True))

    def test_a_daemon_that_gave_no_figure_is_refused_in_both_modes(self) -> None:
        for host in (False, True):
            with self.subTest(host=host):
                self.assertEqual("daemon_memory", memory_problem(0, 100 * 1024, self.NEEDED, host))


class LimitsTest(unittest.TestCase):
    def test_the_local_figures_are_the_ones_that_the_runners_had_before_the_host_mode(self) -> None:
        self.assertEqual((180, 3000, 3600, 3500, 300, 30), dataclasses.astuple(LOCAL_LIMITS)[:6])

    def test_the_host_figures_are_stated_here(self) -> None:
        self.assertEqual((180, 1800, 1800, 1700, 300, 30, 5400), dataclasses.astuple(HOST_LIMITS))

    def test_the_mode_picks_its_own_figures(self) -> None:
        self.assertIs(HOST_LIMITS, limits_for(True))
        self.assertIs(LOCAL_LIMITS, limits_for(False))

    def test_every_figure_is_a_positive_whole_number_of_seconds(self) -> None:
        for limits in (LOCAL_LIMITS, HOST_LIMITS):
            for field in dataclasses.fields(limits):
                value = getattr(limits, field.name)
                if value is not None:
                    with self.subTest(field=field.name):
                        self.assertIsInstance(value, int)
                        self.assertGreater(value, 0)

    def test_a_job_deadline_is_shorter_than_the_runners_outer_limit_so_the_job_reports_its_own_timeout(self) -> None:
        for limits in (LOCAL_LIMITS, HOST_LIMITS):
            self.assertLess(limits.job_deadline_seconds, limits.job_seconds)
            self.assertGreaterEqual(limits.job_seconds - limits.job_deadline_seconds, 60)

    def test_no_host_step_waits_longer_than_the_same_step_locally(self) -> None:
        for name in ("command_seconds", "build_seconds", "job_seconds", "job_deadline_seconds", "health_seconds", "call_seconds"):
            with self.subTest(step=name):
                self.assertLessEqual(getattr(HOST_LIMITS, name), getattr(LOCAL_LIMITS, name))

    def test_the_local_mode_has_no_run_deadline_and_the_host_mode_has_one_that_holds_a_whole_job_more(self) -> None:
        self.assertIsNone(LOCAL_LIMITS.run_seconds)
        self.assertGreater(HOST_LIMITS.run_seconds or 0, HOST_LIMITS.job_seconds)

    def test_the_run_ceiling_is_the_run_deadline_plus_one_job_because_a_job_that_starts_before_the_deadline_may_run_to_its_limit(self) -> None:
        self.assertEqual(7200, HOST_LIMITS.ceiling_seconds)
        self.assertIsNone(LOCAL_LIMITS.ceiling_seconds)


class RunDeadlineTest(unittest.TestCase):
    def test_the_host_run_expires_at_its_deadline_and_not_before(self) -> None:
        deadline = HOST_LIMITS.run_seconds or 0
        self.assertGreater(deadline, 0)
        self.assertFalse(run_expired(100.0, 100.0 + deadline - 0.001, HOST_LIMITS))
        self.assertTrue(run_expired(100.0, 100.0 + deadline, HOST_LIMITS))

    def test_the_local_run_never_expires(self) -> None:
        self.assertFalse(run_expired(0.0, 10.0**9, LOCAL_LIMITS))

    def test_a_clock_that_goes_back_does_not_expire_the_run(self) -> None:
        self.assertFalse(run_expired(500.0, 100.0, HOST_LIMITS))


if __name__ == "__main__":
    unittest.main()
