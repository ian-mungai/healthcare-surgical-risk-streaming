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
- The caller's own environment reached Compose (an exported ``WAREHOUSE_LOCATION``, ``NESSIE_URI`` or ``COMPOSE_*`` setting beat the run's env files and
  pointed Spark at another catalog or warehouse).
- A wait ran past the run deadline (a job, a build or a health wait got its full limit with only seconds of the run left), the cleanup had no bound of its
  own, or the job's own deadline was longer than the cap that it was given.
- A given Spark image was taken by its tag only (the tag moved, or named another build than the tested one) and nothing recorded its immutable ID.
- The host check counted the services' limits only and not the host's reserve in ``compose/limits.yml``.
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
    active_cap,
    available_problem,
    cleanup_cap,
    docker_environment,
    host_flag,
    image_choice,
    image_id_problem,
    job_deadline_for,
    limits_for,
    memory_problem,
    pinned_folders,
    read_env_file,
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
    TAG = "surgical-risk-spark:p6-sig-1a2b3c4d"
    IMAGE_ID = "sha256:" + "ab" * 32

    def test_without_a_given_image_the_run_builds_its_own_tag_and_has_no_source(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, True, None, None), image_choice({}, self.TAG))

    def test_an_empty_image_setting_is_not_a_given_image(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, True, None, None), image_choice({"SPARK_IMAGE": "", "SPARK_IMAGE_ID": ""}, self.TAG))

    def test_a_given_image_keeps_the_run_tag_and_names_its_source_and_the_id_that_the_deploy_recorded(self) -> None:
        given = {"SPARK_IMAGE": "surgical-risk-spark:4.1.4", "SPARK_IMAGE_ID": self.IMAGE_ID}
        self.assertEqual(host_mode.Image(self.TAG, False, "surgical-risk-spark:4.1.4", self.IMAGE_ID), image_choice(given, self.TAG))

    def test_a_given_image_without_the_recorded_id_is_refused_because_a_tag_can_name_another_build(self) -> None:
        with self.assertRaises(HostSettingError) as caught:
            image_choice({"SPARK_IMAGE": "surgical-risk-spark:4.1.4"}, self.TAG)
        self.assertEqual("SPARK_IMAGE_ID is not set", str(caught.exception))

    def test_an_id_that_is_not_a_full_sha256_image_id_is_refused_and_the_text_never_holds_the_value(self) -> None:
        for value in ("sha256:abc", "ab" * 32, "sha256:" + "AB" * 32, "sha256:" + "ab" * 33, "sha256:" + "zz" * 32, "surgical-risk-spark:4.1.4", "--private"):
            with self.subTest(value=value), self.assertRaises(HostSettingError) as caught:
                image_choice({"SPARK_IMAGE": "surgical-risk-spark:4.1.4", "SPARK_IMAGE_ID": value}, self.TAG)
            self.assertEqual("SPARK_IMAGE_ID is not an image ID (sha256: and 64 hexadecimal characters)", str(caught.exception))

    def test_an_image_id_without_an_image_is_refused_so_that_it_is_not_silently_ignored(self) -> None:
        with self.assertRaises(HostSettingError) as caught:
            image_choice({"SPARK_IMAGE_ID": self.IMAGE_ID}, self.TAG)
        self.assertEqual("SPARK_IMAGE_ID is set without SPARK_IMAGE", str(caught.exception))

    def test_a_reference_that_docker_could_read_as_an_option_or_as_two_words_is_refused(self) -> None:
        for value in ("--rm", "-v", "a b", "image;rm", "image:", "UPPER:tag", "a:b:c", "image\n", "/abs/path"):
            with self.subTest(value=value), self.assertRaises(HostSettingError) as caught:
                image_choice({"SPARK_IMAGE": value, "SPARK_IMAGE_ID": self.IMAGE_ID}, self.TAG)
            self.assertEqual("SPARK_IMAGE is not an image name with a tag", str(caught.exception))

    def test_a_registry_name_with_a_port_and_a_tag_is_accepted(self) -> None:
        given = {"SPARK_IMAGE": "registry.local:5000/team/spark:1.2", "SPARK_IMAGE_ID": self.IMAGE_ID}
        self.assertEqual("registry.local:5000/team/spark:1.2", image_choice(given, self.TAG).source)

    def test_a_name_without_a_tag_is_refused_because_it_would_follow_latest(self) -> None:
        with self.assertRaises(HostSettingError):
            image_choice({"SPARK_IMAGE": "surgical-risk-spark", "SPARK_IMAGE_ID": self.IMAGE_ID}, self.TAG)


class ImageIdTest(unittest.TestCase):
    ID = "sha256:" + "cd" * 32

    def test_the_id_that_the_deploy_recorded_is_the_only_one_that_passes(self) -> None:
        self.assertIsNone(image_id_problem(self.ID, self.ID))
        self.assertEqual("image_id_mismatch", image_id_problem("sha256:" + "ce" * 32, self.ID))

    def test_an_id_that_docker_did_not_give_is_never_a_match(self) -> None:
        for actual in ("", "<no value>", "cd" * 32, self.ID + "\n"):
            with self.subTest(actual=actual):
                self.assertEqual("image_id_unreadable", image_id_problem(actual, self.ID))


class DockerEnvironmentTest(unittest.TestCase):
    CALLER = {
        "PATH": "/usr/bin",
        "HOME": "/var/runner",
        "DOCKER_HOST": "unix:///run/docker.sock",
        "DOCKER_CONTEXT": "default",
        "WAREHOUSE_LOCATION": "s3://deployed/warehouse",
        "NESSIE_URI": "http://deployed:19120/api/v2",
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "key",
        "COMPOSE_PROJECT_NAME": "deployed",
        "COMPOSE_FILE": "/deployed/compose.yml",
        "COMPOSE_PROFILES": "all",
        "MEM_LIMIT_SPARK": "1m",
        "SPARK_IMAGE": "other:1",
        "DOCKER_DEFAULT_PLATFORM": "linux/arm64",
    }

    def test_only_the_path_the_home_and_what_docker_itself_needs_come_from_the_caller(self) -> None:
        result = docker_environment(self.CALLER)
        self.assertEqual({"PATH": "/usr/bin", "HOME": "/var/runner", "DOCKER_HOST": "unix:///run/docker.sock", "DOCKER_CONTEXT": "default"}, result)

    def test_no_deployment_or_compose_setting_of_the_caller_reaches_the_result(self) -> None:
        names = set(docker_environment(self.CALLER))
        for name in (
            "WAREHOUSE_LOCATION",
            "NESSIE_URI",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "COMPOSE_PROJECT_NAME",
            "COMPOSE_FILE",
            "COMPOSE_PROFILES",
            "MEM_LIMIT_SPARK",
        ):
            self.assertNotIn(name, names)

    def test_the_run_binding_is_added_and_beats_a_caller_value_of_the_same_name(self) -> None:
        binding = {"WAREHOUSE_LOCATION": "file:///warehouse/", "NESSIE_URI": "http://nessie:19120/api/v2", "HOME": "/var/runner_other"}
        result = docker_environment(self.CALLER, binding)
        self.assertEqual("file:///warehouse/", result["WAREHOUSE_LOCATION"])
        self.assertEqual("http://nessie:19120/api/v2", result["NESSIE_URI"])
        self.assertEqual("/var/runner_other", result["HOME"])

    def test_a_caller_without_a_path_gets_none_and_the_result_never_holds_an_empty_made_up_value(self) -> None:
        self.assertEqual({}, docker_environment({"WAREHOUSE_LOCATION": "x"}))


class EnvFileTest(unittest.TestCase):
    def test_each_line_is_a_name_and_its_value_and_a_comment_or_blank_line_is_skipped(self) -> None:
        text = "# note\n\nA=1\nB=file:///w/\nC=\nD=x=y\n"
        self.assertEqual({"A": "1", "B": "file:///w/", "C": "", "D": "x=y"}, read_env_file(text))

    def test_a_line_without_a_name_and_an_equals_sign_is_ignored(self) -> None:
        self.assertEqual({"A": "1"}, read_env_file("junk\n=novalue\nA=1\n"))


class MemoryTest(unittest.TestCase):
    NEEDED = 6 * GIB
    RESERVE = 4 * GIB

    def test_a_local_run_needs_twice_the_needed_memory_in_the_daemon_and_no_free_figure(self) -> None:
        self.assertIsNone(memory_problem(12 * GIB, None, self.NEEDED, False, self.RESERVE))
        self.assertEqual("daemon_memory", memory_problem(12 * GIB - 1, None, self.NEEDED, False, self.RESERVE))

    def test_a_host_run_needs_the_daemon_memory_and_the_needed_memory_and_the_reserve_free(self) -> None:
        free = (self.NEEDED + self.RESERVE) // (1024 * 1024)
        self.assertIsNone(memory_problem(32 * GIB, free, self.NEEDED, True, self.RESERVE))
        self.assertEqual("available_memory", memory_problem(32 * GIB, free - 1, self.NEEDED, True, self.RESERVE))

    def test_the_reserve_is_what_makes_the_difference_between_enough_and_not_enough(self) -> None:
        self.assertIsNone(memory_problem(32 * GIB, 6 * 1024, self.NEEDED, True, 0))
        self.assertEqual("available_memory", memory_problem(32 * GIB, 6 * 1024, self.NEEDED, True, self.RESERVE))

    def test_a_host_run_with_no_free_memory_figure_is_refused_never_assumed_to_fit(self) -> None:
        self.assertEqual("available_unknown", memory_problem(32 * GIB, None, self.NEEDED, True, self.RESERVE))

    def test_the_daemon_total_is_checked_first_on_the_host(self) -> None:
        self.assertEqual("daemon_memory", memory_problem(8 * GIB, 100 * 1024, self.NEEDED, True, self.RESERVE))

    def test_a_daemon_that_gave_no_figure_is_refused_in_both_modes(self) -> None:
        for host in (False, True):
            with self.subTest(host=host):
                self.assertEqual("daemon_memory", memory_problem(0, 100 * 1024, self.NEEDED, host, self.RESERVE))

    def test_the_fresh_reading_before_the_services_start_holds_the_same_figure_and_never_assumes_a_missing_one(self) -> None:
        free = (self.NEEDED + self.RESERVE) // (1024 * 1024)
        self.assertIsNone(available_problem(free, self.NEEDED, self.RESERVE))
        self.assertEqual("available_memory", available_problem(free - 1, self.NEEDED, self.RESERVE))
        self.assertEqual("available_unknown", available_problem(None, self.NEEDED, self.RESERVE))


class LimitsTest(unittest.TestCase):
    def test_the_local_figures_are_the_ones_that_the_runners_had_before_the_host_mode(self) -> None:
        self.assertEqual((180, 3000, 3600, 3500, 300, 30), dataclasses.astuple(LOCAL_LIMITS)[:6])

    def test_the_host_figures_are_stated_here(self) -> None:
        self.assertEqual((180, 1800, 1800, 1700, 300, 30, 5400, 600), dataclasses.astuple(HOST_LIMITS))

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

    def test_the_run_ceiling_is_the_run_deadline_plus_the_cleanup_budget_because_no_wait_goes_past_the_deadline(self) -> None:
        self.assertEqual(6000, HOST_LIMITS.ceiling_seconds)
        self.assertIsNone(LOCAL_LIMITS.ceiling_seconds)

    def test_the_cleanup_budget_is_stated_in_both_modes_and_is_never_shorter_than_one_command(self) -> None:
        for limits in (LOCAL_LIMITS, HOST_LIMITS):
            self.assertGreaterEqual(limits.cleanup_seconds, limits.command_seconds)


class ActiveCapTest(unittest.TestCase):
    def test_a_step_gets_its_own_limit_while_the_rest_of_the_run_is_longer(self) -> None:
        self.assertEqual(1800, active_cap(100.0, 100.0 + 60, HOST_LIMITS, 1800))

    def test_a_step_gets_only_the_rest_of_the_run_when_that_is_shorter(self) -> None:
        deadline = HOST_LIMITS.run_seconds or 0
        self.assertEqual(500, active_cap(100.0, 100.0 + deadline - 500, HOST_LIMITS, 1800))

    def test_a_step_that_would_start_at_or_after_the_deadline_gets_no_cap(self) -> None:
        deadline = HOST_LIMITS.run_seconds or 0
        self.assertIsNone(active_cap(100.0, 100.0 + deadline, HOST_LIMITS, 30))
        self.assertIsNone(active_cap(100.0, 100.0 + deadline + 1, HOST_LIMITS, 30))

    def test_the_local_run_has_no_deadline_and_every_step_keeps_its_own_limit(self) -> None:
        self.assertEqual(3600, active_cap(0.0, 10.0**9, LOCAL_LIMITS, 3600))

    def test_a_clock_that_goes_back_does_not_shorten_the_step(self) -> None:
        self.assertEqual(1800, active_cap(500.0, 100.0, HOST_LIMITS, 1800))

    def test_the_cap_of_a_short_step_near_the_end_is_the_short_remainder(self) -> None:
        deadline = HOST_LIMITS.run_seconds or 0
        self.assertAlmostEqual(0.5, active_cap(0.0, deadline - 0.5, HOST_LIMITS, 180) or 0)


class CleanupCapTest(unittest.TestCase):
    def test_the_first_cleanup_step_gets_its_own_limit(self) -> None:
        self.assertEqual(180, cleanup_cap(None, 1000.0, HOST_LIMITS, 180))

    def test_a_step_gets_only_what_is_left_of_the_cleanup_budget(self) -> None:
        self.assertEqual(100, cleanup_cap(1000.0, 1000.0 + HOST_LIMITS.cleanup_seconds - 100, HOST_LIMITS, 180))

    def test_a_spent_budget_starts_no_further_step(self) -> None:
        self.assertIsNone(cleanup_cap(1000.0, 1000.0 + HOST_LIMITS.cleanup_seconds, HOST_LIMITS, 180))

    def test_the_budget_is_the_same_whether_or_not_the_run_has_a_deadline(self) -> None:
        self.assertIsNone(cleanup_cap(0.0, float(LOCAL_LIMITS.cleanup_seconds), LOCAL_LIMITS, 180))

    def test_the_cleanup_budget_does_not_depend_on_the_run_deadline_having_passed(self) -> None:
        self.assertEqual(180, cleanup_cap(10.0**6, 10.0**6 + 1, HOST_LIMITS, 180))


class JobDeadlineTest(unittest.TestCase):
    def test_a_job_with_its_full_limit_keeps_its_own_deadline(self) -> None:
        self.assertEqual(1700, job_deadline_for(1800, HOST_LIMITS))
        self.assertEqual(1700, job_deadline_for(5000, HOST_LIMITS))

    def test_a_job_with_less_time_keeps_the_same_margin_before_the_cap_so_that_it_reports_its_own_timeout(self) -> None:
        self.assertEqual(400, job_deadline_for(500, HOST_LIMITS))

    def test_a_cap_that_is_no_longer_than_the_margin_starts_no_job(self) -> None:
        margin = HOST_LIMITS.job_seconds - HOST_LIMITS.job_deadline_seconds
        self.assertIsNone(job_deadline_for(margin, HOST_LIMITS))
        self.assertIsNone(job_deadline_for(0.5, HOST_LIMITS))
        self.assertEqual(1, job_deadline_for(margin + 1, HOST_LIMITS))


if __name__ == "__main__":
    unittest.main()
