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
- A given Spark image with the recorded ID was taken as the tested build though the files in it were not the files of the reviewed commit (the ID alone binds
  nothing to a revision), or the check passed on a listing that held no file.
- On the host the checkout has no Git history (``git ls-tree`` and ``git show`` exit 128), so the image check failed closed on every run. It reads the
  commit's files from the deploy's source manifest instead: a file that differs from its manifest digest, a file that the manifest does not list or that is
  gone, a link, a path that could forge a listing line and a Dockerfile or ignore file that is not checked must never give a listing.
- A call to Nessie got its own 30 seconds with only seconds of the run left, because the rest of the run was read before the call and not at its start.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import os
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.process import run_command
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
    remaining_seconds,
    source_problem,
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
            self.assertEqual("SPARK_IMAGE is not an image name with a tag or a digest", str(caught.exception))

    def test_a_registry_name_with_a_port_and_a_tag_is_accepted(self) -> None:
        given = {"SPARK_IMAGE": "registry.local:5000/team/spark:1.2", "SPARK_IMAGE_ID": self.IMAGE_ID}
        self.assertEqual("registry.local:5000/team/spark:1.2", image_choice(given, self.TAG).source)

    def test_a_name_without_a_tag_is_refused_because_it_would_follow_latest(self) -> None:
        with self.assertRaises(HostSettingError):
            image_choice({"SPARK_IMAGE": "surgical-risk-spark", "SPARK_IMAGE_ID": self.IMAGE_ID}, self.TAG)


class PinnedImageChoiceTest(unittest.TestCase):
    """The host runs the digest that ``compose/image_pins.yml`` names (the host builds nothing and has no tag of the publisher's)."""

    TAG = "surgical-risk-spark:p6-sig-1a2b3c4d"
    IMAGE_ID = "sha256:" + "ab" * 32
    PINNED = "ianmungai/healthcare_surgical_risk_streaming@sha256:" + "cd" * 32

    def test_a_pinned_digest_reference_is_a_given_image_and_the_id_is_the_matching_one_that_is_given(self) -> None:
        given = {"SPARK_IMAGE": self.PINNED, "SPARK_IMAGE_ID": self.IMAGE_ID}
        self.assertEqual(host_mode.Image(self.TAG, False, self.PINNED, self.IMAGE_ID), image_choice(given, self.TAG))

    def test_a_digest_reference_names_its_content_so_it_needs_no_id_from_the_operator(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, False, self.PINNED, None), image_choice({"SPARK_IMAGE": self.PINNED}, self.TAG))

    def test_an_id_that_is_given_with_a_digest_reference_is_still_checked_for_its_form(self) -> None:
        with self.assertRaises(HostSettingError) as caught:
            image_choice({"SPARK_IMAGE": self.PINNED, "SPARK_IMAGE_ID": "sha256:abc"}, self.TAG)
        self.assertEqual("SPARK_IMAGE_ID is not an image ID (sha256: and 64 hexadecimal characters)", str(caught.exception))

    def test_a_digest_reference_of_another_form_is_refused_and_the_text_never_holds_the_value(self) -> None:
        values = (
            "repo@sha256:abc",
            "repo@sha256:" + "CD" * 32,
            "repo@sha256:" + "cd" * 33,
            "repo@md5:" + "cd" * 16,
            "@sha256:" + "cd" * 32,
            "repo@",
            "re po@sha256:" + "cd" * 32,
        )
        for value in values:
            with self.subTest(value=value), self.assertRaises(HostSettingError) as caught:
                image_choice({"SPARK_IMAGE": value}, self.TAG)
            self.assertEqual("SPARK_IMAGE is not an image name with a tag or a digest", str(caught.exception))

    def test_a_host_that_is_given_the_pinned_reference_runs_it_without_any_setting_from_the_operator(self) -> None:
        self.assertEqual(host_mode.Image(self.TAG, False, self.PINNED, None), image_choice({}, self.TAG, pinned=self.PINNED))
        self.assertEqual(
            host_mode.Image(self.TAG, False, self.PINNED, self.IMAGE_ID), image_choice({"SPARK_IMAGE_ID": self.IMAGE_ID}, self.TAG, pinned=self.PINNED)
        )

    def test_an_image_that_the_operator_gives_beats_the_pinned_reference(self) -> None:
        given = {"SPARK_IMAGE": "surgical-risk-spark:4.1.4", "SPARK_IMAGE_ID": self.IMAGE_ID}
        self.assertEqual("surgical-risk-spark:4.1.4", image_choice(given, self.TAG, pinned=self.PINNED).source)

    def test_a_pinned_reference_that_is_not_a_digest_is_refused(self) -> None:
        with self.assertRaises(HostSettingError) as caught:
            image_choice({}, self.TAG, pinned="surgical-risk-spark:4.1.4")
        self.assertEqual("the pinned Spark image is not a digest reference", str(caught.exception))

    def test_without_a_recorded_id_the_run_only_needs_a_readable_one_from_docker(self) -> None:
        self.assertIsNone(image_id_problem("sha256:" + "ab" * 32, None))
        self.assertEqual("image_id_unreadable", image_id_problem("", None))
        self.assertEqual("image_id_unreadable", image_id_problem("sha256:abc\n", None))


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


class RemainingTest(unittest.TestCase):
    def test_a_run_with_a_deadline_has_the_rest_of_it_left_at_every_reading(self) -> None:
        self.assertEqual(5400.0, remaining_seconds(100.0, 100.0, HOST_LIMITS))
        self.assertEqual(5300.0, remaining_seconds(100.0, 200.0, HOST_LIMITS))
        self.assertEqual(20.0, remaining_seconds(0.0, 5380.0, HOST_LIMITS))

    def test_a_run_past_its_deadline_has_nothing_left_never_a_negative_figure(self) -> None:
        self.assertEqual(0.0, remaining_seconds(0.0, 5400.0, HOST_LIMITS))
        self.assertEqual(0.0, remaining_seconds(0.0, 10.0**6, HOST_LIMITS))

    def test_a_clock_that_goes_back_does_not_lengthen_the_run(self) -> None:
        self.assertEqual(5400.0, remaining_seconds(100.0, 50.0, HOST_LIMITS))

    def test_a_local_run_has_no_deadline_so_nothing_is_left_to_count(self) -> None:
        self.assertIsNone(remaining_seconds(0.0, 10.0**6, LOCAL_LIMITS))


SHA_X, SHA_Y, SHA_Z, SHA_V, SHA_C = "a1" * 20, "b2" * 20, "c3" * 20, "f7" * 20, "9a" * 20
ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = (
    "FROM python:3.12\n"
    "COPY --from=jre /opt/java/openjdk /opt/java/openjdk\n"
    "COPY spark/requirements.txt /tmp/requirements.txt\n"
    "COPY spark /opt/jobs/spark\n"
    "COPY scripts/stack/nessie.py /opt/jobs/scripts/stack/nessie.py\n"
    "# a comment\n"
    "COPY features/signal_validity.py \\\n    /opt/jobs/features/signal_validity.py\n"
    "COPY --chown=1:1 casefile /opt/jobs/casefile\n"
    "WORKDIR /opt/jobs\n"
)
TREE = (
    f"100644 blob {SHA_X}\tspark/jobs/x.py\n100644 blob {SHA_Y}\tspark/silver/y.py\n100644 blob {SHA_Z}\tscripts/stack/nessie.py\n"
    f"100644 blob {SHA_V}\tfeatures/signal_validity.py\n100644 blob {SHA_C}\tcasefile/store.py\n100644 blob {'a7' * 20}\tcasefile/data.json\n"
    f"100644 blob {'d4' * 20}\tspark/Dockerfile\n100644 blob {'e5' * 20}\tREADME.md\n100644 blob {'b8' * 20}\tspark/requirements.txt\n"
)
# The Docker ignore file of the same build: everything is out except what is named (the form of ``spark/Dockerfile.dockerignore``).
DOCKERIGNORE = "# a comment\n\n*\n!spark\n!scripts/stack/nessie.py\n!features/signal_validity.py\n!casefile\n**/__pycache__\n"
IMAGE_FILES = (
    f"{SHA_C} casefile/store.py\n{SHA_V} features/signal_validity.py\n{SHA_Z} scripts/stack/nessie.py\n{SHA_X} spark/jobs/x.py\n{SHA_Y} spark/silver/y.py\n"
)


def committed(*arguments: str) -> str:
    """What git says of the commit under test (HEAD of this checkout)."""
    return run_command("git", ["-C", str(ROOT), *arguments], timeout=60, check=True).stdout


class SourceProblemTest(unittest.TestCase):
    def test_an_image_that_holds_exactly_the_committed_files_it_copies_is_the_tested_build(self) -> None:
        self.assertIsNone(source_problem(IMAGE_FILES, TREE, DOCKERFILE, DOCKERIGNORE))

    def test_an_image_file_that_differs_from_the_committed_file_is_another_build(self) -> None:
        changed = IMAGE_FILES.replace(SHA_Y, "f6" * 20)
        self.assertEqual("image_source_differs", source_problem(changed, TREE, DOCKERFILE, DOCKERIGNORE))

    def test_an_image_file_that_the_commit_does_not_hold_is_another_build(self) -> None:
        self.assertEqual("image_source_differs", source_problem(IMAGE_FILES + f"{SHA_X} spark/jobs/extra.py\n", TREE, DOCKERFILE, DOCKERIGNORE))

    def test_an_image_file_in_a_folder_that_the_dockerfile_copies_only_in_part_is_another_build(self) -> None:
        self.assertEqual("image_source_differs", source_problem(IMAGE_FILES + f"{SHA_X} features/extra.py\n", TREE, DOCKERFILE, DOCKERIGNORE))
        self.assertEqual("image_source_differs", source_problem(IMAGE_FILES + f"{SHA_X} scripts/stack/extra.py\n", TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_committed_job_file_that_the_image_lacks_is_another_build(self) -> None:
        lacking = IMAGE_FILES.replace(f"{SHA_X} spark/jobs/x.py\n", "")
        self.assertEqual("image_source_missing", source_problem(lacking, TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_committed_file_that_a_single_file_copy_names_and_the_image_lacks_is_another_build(self) -> None:
        for lost in (f"{SHA_V} features/signal_validity.py\n", f"{SHA_Z} scripts/stack/nessie.py\n"):
            with self.subTest(lost=lost):
                self.assertEqual("image_source_missing", source_problem(IMAGE_FILES.replace(lost, ""), TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_committed_file_of_a_copied_folder_other_than_spark_that_the_image_lacks_is_another_build(self) -> None:
        self.assertEqual("image_source_missing", source_problem(IMAGE_FILES.replace(f"{SHA_C} casefile/store.py\n", ""), TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_copied_file_whose_content_changed_in_a_folder_other_than_spark_is_another_build(self) -> None:
        for path in ("casefile/store.py", "features/signal_validity.py", "scripts/stack/nessie.py"):
            with self.subTest(path=path):
                changed = "".join(f"{'0f' * 20 if line.endswith(path) else line.split()[0]} {line.split()[1]}\n" for line in IMAGE_FILES.splitlines())
                self.assertEqual("image_source_differs", source_problem(changed, TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_committed_file_that_the_dockerfile_does_not_copy_is_not_missing(self) -> None:
        extra = TREE + f"100644 blob {'a9' * 20}\tscripts/other.py\n100644 blob {'a8' * 20}\tfeatures/other.py\n"
        self.assertIsNone(source_problem(IMAGE_FILES, extra, DOCKERFILE, DOCKERIGNORE))

    def test_a_file_that_is_not_python_is_not_listed_by_the_image_and_is_not_missing(self) -> None:
        self.assertIsNone(source_problem(IMAGE_FILES, TREE, DOCKERFILE, DOCKERIGNORE))
        self.assertNotIn("data.json", IMAGE_FILES)

    def test_a_listing_with_no_file_is_unreadable_and_never_a_match(self) -> None:
        for listing in ("", "\n", "not a listing line\n", f"{SHA_X}  spark/jobs/x.py\n"):
            with self.subTest(listing=listing):
                self.assertEqual("image_source_unreadable", source_problem(listing, TREE, DOCKERFILE, DOCKERIGNORE))

    def test_a_tree_without_any_file_of_the_dockerfile_is_unreadable_and_never_a_match(self) -> None:
        self.assertEqual("image_source_unreadable", source_problem(IMAGE_FILES, "", DOCKERFILE, DOCKERIGNORE))
        self.assertEqual("image_source_unreadable", source_problem(IMAGE_FILES, f"100644 blob {SHA_Z}\tscripts/stack/nessie.py\n", DOCKERFILE, DOCKERIGNORE))

    def test_a_dockerfile_that_the_check_cannot_read_is_unreadable_and_never_a_match(self) -> None:
        cases = {
            "empty": "",
            "no copy into the jobs folder": "FROM x\nCOPY spark/requirements.txt /tmp/requirements.txt\n",
            "a wildcard source": "COPY spark/*.py /opt/jobs/spark/\n",
            "a JSON form": 'COPY ["spark", "/opt/jobs/spark"]\n',
            "a relative destination": "WORKDIR /opt/jobs\nCOPY spark spark\n",
            "a source that the commit does not hold": "COPY features/gone.py /opt/jobs/features/gone.py\n",
            "a source outside the checkout": "COPY ../secret.py /opt/jobs/secret.py\n",
        }
        for label, text in cases.items():
            with self.subTest(case=label):
                self.assertEqual("image_source_unreadable", source_problem(IMAGE_FILES, TREE, text, DOCKERIGNORE))

    def test_a_copy_from_another_stage_and_a_destination_outside_the_jobs_folder_name_no_file_of_the_jobs_folder(self) -> None:
        both = DOCKERFILE + "COPY --from=build /out/tool.py /opt/jobs/tool.py\nCOPY scripts/stack/nessie.py /tmp/nessie.py\n"
        self.assertIsNone(source_problem(IMAGE_FILES, TREE, both, DOCKERIGNORE))

    def test_a_destination_with_a_trailing_slash_takes_the_name_of_the_source_file(self) -> None:
        text = (
            "COPY spark /opt/jobs/spark\nCOPY features/signal_validity.py /opt/jobs/features/\n"
            "COPY scripts/stack/nessie.py /opt/jobs/scripts/stack/\nCOPY casefile /opt/jobs/casefile\n"
        )
        self.assertIsNone(source_problem(IMAGE_FILES, TREE, text, DOCKERIGNORE))

    def test_a_later_copy_to_the_same_path_decides_which_file_the_image_holds(self) -> None:
        text = DOCKERFILE + "COPY scripts/stack/nessie.py /opt/jobs/features/signal_validity.py\n"
        self.assertEqual("image_source_differs", source_problem(IMAGE_FILES, TREE, text, DOCKERIGNORE))
        swapped = IMAGE_FILES.replace(f"{SHA_V} features", f"{SHA_Z} features")
        self.assertIsNone(source_problem(swapped, TREE, text, DOCKERIGNORE))

    def test_the_files_of_the_real_dockerfile_are_checked_in_every_folder_it_copies_from(self) -> None:
        tree = committed("ls-tree", "-r", "HEAD")
        dockerfile = committed("show", "HEAD:spark/Dockerfile")
        dockerignore = committed("show", "HEAD:spark/Dockerfile.dockerignore")
        found = host_mode.image_source_files(dockerfile, tree, dockerignore)
        self.assertIsNotNone(found)
        files = found or {}
        for folder in ("spark/", "features/", "casefile/", "scripts/", "replay/"):
            self.assertTrue(any(path.startswith(folder) for path in files), folder)
        self.assertIn("features/signal_validity.py", files)
        listing = "".join(f"{blob} {path}\n" for path, blob in sorted(files.items()))
        self.assertIsNone(source_problem(listing, tree, dockerfile, dockerignore))
        for lost in ("features/signal_validity.py", "scripts/stack/nessie.py", "replay/events/wire.py", "casefile/__init__.py"):
            with self.subTest(lost=lost):
                lacking = "".join(f"{blob} {path}\n" for path, blob in sorted(files.items()) if path != lost)
                self.assertEqual("image_source_missing", source_problem(lacking, tree, dockerfile, dockerignore))
        self.assertEqual("image_source_differs", source_problem(listing + f"{SHA_X} features/extra.py\n", tree, dockerfile, dockerignore))

    def test_the_real_ignore_file_keeps_the_command_line_module_out_of_the_image_and_an_image_that_holds_it_is_another_build(self) -> None:
        tree = committed("ls-tree", "-r", "HEAD")
        dockerfile = committed("show", "HEAD:spark/Dockerfile")
        dockerignore = committed("show", "HEAD:spark/Dockerfile.dockerignore")
        self.assertIn("scripts/ingestion/cli.py", dockerignore.splitlines())
        committed_blobs = {path: blob for line in tree.splitlines() if (found := host_mode.TREE_LINE.fullmatch(line)) for blob, path in [found.groups()]}
        files = host_mode.image_source_files(dockerfile, tree, dockerignore) or {}
        self.assertNotIn("scripts/ingestion/cli.py", files)
        self.assertIn("scripts/ingestion/check.py", files)
        self.assertEqual(174, len(files))
        listing = "".join(f"{blob} {path}\n" for path, blob in sorted(files.items()))
        self.assertIsNone(source_problem(listing, tree, dockerfile, dockerignore))
        with_cli = listing + f"{committed_blobs['scripts/ingestion/cli.py']} scripts/ingestion/cli.py\n"
        self.assertEqual("image_source_differs", source_problem(with_cli, tree, dockerfile, dockerignore))


def tree_of(*paths: str) -> str:
    """A ``git ls-tree -r`` listing of the given paths, each with a blob ID of its own (the position, repeated)."""
    return "".join(f"100644 blob {f'{index:02x}' * 20}\t{path}\n" for index, path in enumerate(paths, 1))


def kept(ignore: str, dockerfile: str, *paths: str) -> list[str] | None:
    """The paths under /opt/jobs that an image built from these files and this Docker ignore text holds (sorted), or None when the check cannot read them."""
    found = host_mode.image_source_files(dockerfile, tree_of(*paths), ignore)
    return None if found is None else sorted(found)


APP = "COPY app /opt/jobs/app\n"
HASH_COPY = "COPY #b.py /opt/jobs/b.py\n"


class DockerIgnoreTest(unittest.TestCase):
    """The Docker ignore file is applied to the build context before ``COPY`` reads it. The rules are those of the Docker documentation, "Build context",
    ".dockerignore files" (https://docs.docker.com/build/concepts/context/#dockerignore-files): a pattern is read from the root of the context, ``*`` matches
    any characters of one name, a leading ``**/`` matches any number of folders, a line that starts with ``!`` is an exception, a line whose first character is
    ``#`` (after a byte order mark on the first line, and with no white space before it) is a comment, and the last line that matches a path decides. The lines
    are read as ``ReadAll`` of moby's ``ignorefile`` package reads them at Docker 29.9.3
    (https://github.com/moby/moby/blob/docker-v29.9.3/vendor/github.com/moby/patternmatcher/ignorefile/ignorefile.go#L24-L73). A path is also matched by its
    folders (a rule for a folder is a rule for what is in it)."""

    def test_a_file_that_no_rule_names_is_in_the_image_and_an_empty_ignore_file_has_no_rule(self) -> None:
        for ignore in ("", "\n\n", "# only a comment\n", "  \n"):
            with self.subTest(ignore=ignore):
                self.assertEqual(["app/a.py", "app/sub/b.py"], kept(ignore, APP, "app/a.py", "app/sub/b.py", "other/c.py"))

    def test_a_path_rule_leaves_out_that_file_only(self) -> None:
        self.assertEqual(["app/b.py"], kept("app/a.py\n", APP, "app/a.py", "app/b.py", "other/a.py"))

    def test_a_rule_for_a_folder_leaves_out_everything_in_it(self) -> None:
        self.assertEqual(["app/a.py"], kept("app/sub\n", APP, "app/a.py", "app/sub/b.py", "app/sub/deep/c.py"))

    def test_a_star_alone_leaves_out_everything_in_the_context_so_that_a_copy_of_it_has_no_source(self) -> None:
        self.assertIsNone(kept("*\n", APP, "app/a.py", "app/sub/b.py"))

    def test_a_star_matches_one_name_and_never_crosses_a_slash(self) -> None:
        self.assertEqual(["app/sub/b.py"], kept("app/*\n!app/sub\n", APP, "app/a.py", "app/sub/b.py"))
        self.assertEqual(["app/sub/b.py"], kept("*/a.py\n", APP, "app/a.py", "app/sub/b.py"))
        self.assertEqual(["app/a.py", "app/sub/a.py", "app/sub/b.py"], kept("*/b.py\n", APP, "app/a.py", "app/b.py", "app/sub/a.py", "app/sub/b.py"))

    def test_a_rule_with_a_star_in_the_middle_matches_one_folder_level_only(self) -> None:
        self.assertEqual(["app/x/keep/a.py"], kept("app/*/drop\n", "COPY app /opt/jobs/app\n", "app/x/drop/a.py", "app/x/keep/a.py"))
        self.assertEqual(["app/x/y/drop/a.py"], kept("app/*/drop\n", "COPY app /opt/jobs/app\n", "app/x/drop/a.py", "app/x/y/drop/a.py"))

    def test_a_leading_double_star_matches_the_name_at_the_root_and_at_any_depth(self) -> None:
        paths = ("app/a.py", "app/__pycache__/m.py", "app/sub/__pycache__/m.py", "app/sub/deep/__pycache__/x/m.py")
        self.assertEqual(["app/a.py"], kept("**/__pycache__\n", APP, *paths))
        self.assertIsNone(kept("**/__pycache__\n", "COPY __pycache__/m.py /opt/jobs/m.py\n", "__pycache__/m.py"))
        self.assertEqual(["app/__pycache__/m.py"], kept("**/sub\n", APP, "app/__pycache__/m.py", "app/sub/a.py", "app/x/sub/b.py"))

    def test_a_path_rule_is_read_from_the_root_so_the_same_name_deeper_down_stays(self) -> None:
        self.assertEqual(["app/other/a.py"], kept("a.py\napp/a.py\n", APP, "app/a.py", "app/other/a.py"))

    def test_an_exception_puts_back_what_an_earlier_rule_left_out(self) -> None:
        self.assertEqual(["app/a.py", "app/sub/keep.py"], kept("app/sub\n!app/sub/keep.py\n", APP, "app/a.py", "app/sub/keep.py", "app/sub/drop.py"))
        self.assertEqual(["app/a.py", "app/sub/b.py", "app/sub/c.py"], kept("app/sub/*\n!app/sub\n", APP, "app/a.py", "app/sub/b.py", "app/sub/c.py"))

    def test_the_last_matching_line_decides_so_an_exception_before_the_rule_changes_nothing(self) -> None:
        self.assertEqual(["app/a.py"], kept("!app/sub/keep.py\napp/sub\n", APP, "app/a.py", "app/sub/keep.py"))
        self.assertEqual(["app/a.py"], kept("app/sub\n!app/sub/keep.py\napp/sub/keep.py\n", APP, "app/a.py", "app/sub/keep.py"))

    def test_everything_out_first_and_a_named_folder_back_is_the_form_of_the_real_file(self) -> None:
        ignore = "*\n!app\n!lib/one.py\nlib/one.py\n!lib/two.py\n"
        dockerfile = APP + "COPY lib/two.py /opt/jobs/lib/two.py\n"
        self.assertEqual(["app/a.py", "app/sub/b.py", "lib/two.py"], kept(ignore, dockerfile, "app/a.py", "app/sub/b.py", "lib/one.py", "lib/two.py", "x/y.py"))

    def test_a_single_file_copy_of_a_file_that_the_ignore_file_leaves_out_has_no_source(self) -> None:
        text = "COPY lib/one.py /opt/jobs/lib/one.py\n"
        self.assertIsNone(kept("lib/one.py\n", text, "lib/one.py"))
        self.assertEqual(["lib/one.py"], kept("lib/one.py\n!lib/one.py\n", text, "lib/one.py"))

    def test_comments_blank_lines_and_the_white_space_and_line_ends_around_a_rule_are_not_part_of_it(self) -> None:
        ignore = "# app/a.py is not a rule\n\n  app/b.py  \r\n\t! app/c.py\r\napp/c.py\r\n! app/c.py\n"
        self.assertEqual(["app/a.py", "app/c.py"], kept(ignore, APP, "app/a.py", "app/b.py", "app/c.py"))

    def test_a_comment_is_only_a_line_that_starts_with_a_hash(self) -> None:
        self.assertEqual(["app/a.py", "app/b.py"], kept("app/b.py # not a comment here\napp/a.py\n!app/a.py\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept("#app/a.py\n#!app/b.py\n", APP, "app/a.py", "app/b.py"))

    def test_a_hash_after_white_space_is_a_pattern_and_not_a_comment_as_the_docker_reader_reads_it(self) -> None:
        for label, ignore in (("a space", " #b.py\n"), ("a tab", "\t#b.py\n"), ("a carriage return", "\r#b.py\n"), ("a no-break space", "\xa0#b.py\n")):
            with self.subTest(before_the_hash=label):
                self.assertIsNone(kept(ignore, HASH_COPY, "#b.py"))
                self.assertEqual(["b.py"], kept(ignore + "!#b.py\n", HASH_COPY, "#b.py"))
        self.assertEqual(["b.py"], kept("#b.py\n", HASH_COPY, "#b.py"))

    def test_a_pattern_that_starts_with_a_hash_after_white_space_and_is_not_read_makes_the_listing_unreadable_and_never_a_comment(self) -> None:
        tree = tree_of("app/a.py", "app/drop.py")
        listing = f"{'01' * 20} app/a.py\n{'02' * 20} app/drop.py\n"
        for ignore in (" #*/drop.py\n", "\t#app/a?.py\n", "  #app/[ab].py\n", "app/a.py\n #**\n"):
            with self.subTest(ignore=ignore):
                self.assertIsNone(kept(ignore, APP, "app/a.py", "app/drop.py"))
                self.assertEqual("image_source_unreadable", source_problem(listing, tree, APP, ignore))

    def test_a_carriage_return_ends_no_line_and_is_part_of_the_line_end_only_before_a_line_feed(self) -> None:
        self.assertEqual(["app/a.py"], kept("app/b.py\r\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept("#app/b.py\r\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept("app/b.py\r\n!app/b.py\r\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept("app/b.py\r\n\r\n! app/b.py \r\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py"], kept("app/b.py\r\r\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py"], kept("app/b.py\r", APP, "app/a.py", "app/b.py"))

    def test_only_a_line_feed_ends_a_line_so_another_line_separator_inside_a_line_is_part_of_its_pattern(self) -> None:
        for mark in ("\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"):
            with self.subTest(mark=ascii(mark)):
                ignore = f"app/a.py{mark}app/b.py\n"
                self.assertEqual(["app/a.py", "app/b.py"], kept(ignore, APP, "app/a.py", "app/b.py"))

    def test_the_white_space_around_a_pattern_is_the_white_space_that_the_docker_reader_trims_and_no_other(self) -> None:
        for mark in (" ", "\t", "\x0b", "\x0c", "\x85", "\xa0", "\u1680", "\u2003", "\u200a", "\u2028", "\u2029", "\u202f", "\u205f", "\u3000"):
            with self.subTest(trimmed=ascii(mark)):
                self.assertEqual(["app/a.py"], kept(f"{mark}app/b.py{mark}\n", APP, "app/a.py", "app/b.py"))
                self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}!{mark}app/b.py\napp/b.py\n{mark}!{mark}app/b.py{mark}\n", APP, "app/a.py", "app/b.py"))
        for mark in ("\x1c", "\x1d", "\x1e", "\x1f", "\u200b", "\u2060"):
            with self.subTest(kept_in_the_name=ascii(mark)):
                self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}app/b.py\n", APP, "app/a.py", "app/b.py"))
                self.assertEqual(["app/a.py", "app/b.py"], kept(f"app/b.py{mark}\n", APP, "app/a.py", "app/b.py"))
                self.assertEqual(["app/a.py"], kept(f"app/b.py\n!{mark}app/b.py\n", APP, "app/a.py", "app/b.py"))

    def test_a_byte_order_mark_is_taken_off_the_first_line_only_and_once(self) -> None:
        mark = "\ufeff"
        for label, ignore in (
            ("before a rule", f"{mark}app/b.py\n"),
            ("before a rule and a carriage return", f"{mark}app/b.py\r\n"),
            ("before a rule that follows a comment", f"{mark}# comment\napp/b.py\n"),
            ("alone on the first line", f"{mark}\napp/b.py\n"),
            ("alone on the first line with a carriage return", f"{mark}\r\napp/b.py\r\n"),
            ("before white space and a rule", f"{mark}  app/b.py\n"),
        ):
            with self.subTest(mark=label):
                self.assertEqual(["app/a.py"], kept(ignore, APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}# comment\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"{mark}{mark}app/a.py\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"app/a.py\n!app/a.py\n{mark}app/b.py\n", APP, "app/a.py", "app/b.py"))
        self.assertEqual(["app/a.py", "app/b.py"], kept(f"\n{mark}app/b.py\n", APP, "app/a.py", "app/b.py"))

    def test_a_byte_order_mark_before_a_rule_is_that_rule_in_the_check_of_the_image(self) -> None:
        tree = tree_of("app/a.py", "app/b.py")
        only_a = f"{'01' * 20} app/a.py\n"
        both = only_a + f"{'02' * 20} app/b.py\n"
        for ignore in ("\ufeffapp/b.py\n", "\ufeff\napp/b.py\n"):
            with self.subTest(ignore=ignore):
                self.assertIsNone(source_problem(only_a, tree, APP, ignore))
                self.assertEqual("image_source_differs", source_problem(both, tree, APP, ignore))
        self.assertEqual("image_source_unreadable", source_problem(both, tree, APP, "\ufeffapp/*.py\n"))

    def test_the_real_ignore_file_is_read_the_same_with_a_byte_order_mark_and_with_line_ends_of_a_carriage_return_and_a_line_feed(self) -> None:
        tree = committed("ls-tree", "-r", "HEAD")
        dockerfile = committed("show", "HEAD:spark/Dockerfile")
        dockerignore = committed("show", "HEAD:spark/Dockerfile.dockerignore")
        expected = host_mode.image_source_files(dockerfile, tree, dockerignore)
        self.assertIsNotNone(expected)
        self.assertEqual(expected, host_mode.image_source_files(dockerfile, tree, "\ufeff" + dockerignore))
        self.assertEqual(expected, host_mode.image_source_files(dockerfile, tree, dockerignore.replace("\n", "\r\n")))
        self.assertEqual(expected, host_mode.image_source_files(dockerfile, tree, "\ufeff" + dockerignore.replace("\n", "\r\n")))

    def test_a_form_that_the_check_does_not_implement_makes_the_listing_unreadable_and_never_a_match(self) -> None:
        forms = {
            "a star with other characters": "app/*.py\n",
            "a star ending a name": "app/a*\n",
            "a question mark": "app/a?.py\n",
            "a character class": "app/[ab].py\n",
            "an escape": "app/a\\.py\n",
            "a double star inside a path": "app/**/a.py\n",
            "a double star at the end": "app/**\n",
            "a double star alone": "**\n",
            "a double star joined to a name": "**a\n",
            "a rooted path": "/app/a.py\n",
            "a path that ends in a slash": "app/sub/\n",
            "a current-folder mark": "./app/a.py\n",
            "a parent mark": "app/../a.py\n",
            "a doubled slash": "app//a.py\n",
            "an exception mark alone": "!\n",
            "an exception of an unread form": "!app/*.py\n",
        }
        tree = tree_of("app/a.py", "app/b.py")
        for label, ignore in forms.items():
            with self.subTest(form=label):
                self.assertIsNone(kept(ignore, APP, "app/a.py", "app/b.py"))
                listing = f"{'a1' * 20} app/a.py\n{'b2' * 20} app/b.py\n"
                self.assertEqual("image_source_unreadable", source_problem(listing, tree, APP, ignore))
                self.assertEqual("image_source_unreadable", source_problem(listing, tree, APP, "app/c.py\n" + ignore))

    def test_the_ignore_file_decides_which_listing_matches_in_the_check_of_the_image(self) -> None:
        tree = tree_of("app/a.py", "app/b.py")
        both = f"{'01' * 20} app/a.py\n{'02' * 20} app/b.py\n"
        only_a = f"{'01' * 20} app/a.py\n"
        self.assertIsNone(source_problem(both, tree, APP, ""))
        self.assertEqual("image_source_missing", source_problem(only_a, tree, APP, ""))
        self.assertIsNone(source_problem(only_a, tree, APP, "app/b.py\n"))
        self.assertEqual("image_source_differs", source_problem(both, tree, APP, "app/b.py\n"))

    def test_the_script_that_the_image_runs_prints_the_git_id_and_the_path_of_each_python_file_under_the_jobs_folder(self) -> None:
        with tempfile.TemporaryDirectory(prefix="host_mode_source_") as folder:
            jobs = Path(folder) / "jobs"
            (jobs / "spark").mkdir(parents=True)
            (jobs / "spark" / "a.py").write_bytes(b"x = 1\n")
            (jobs / "spark" / "note.txt").write_text("not python")
            script = host_mode.SOURCE_SCRIPT.replace("/opt/jobs", str(jobs))
            answered = run_command(sys.executable, ["-I", "-c", script], timeout=30, check=True).stdout
        git_id = "7d4290a117a4ddcc11daae7ea675841033830c8f"  # git hash-object of the file content "x = 1\n"
        self.assertEqual(f"{git_id} spark/a.py\n", answered)


def blob_of(data: bytes) -> str:
    """The Git blob ID of the content (``git hash-object``)."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def manifest_of(files: dict[str, bytes]) -> dict[str, str]:
    """The ``files`` map of a source manifest: the SHA-256 of each file."""
    return {path: hashlib.sha256(data).hexdigest() for path, data in files.items()}


MANIFEST_FILES = {
    "spark/Dockerfile": b"COPY spark /opt/jobs/spark\nCOPY scripts/stack/nessie.py /opt/jobs/scripts/stack/nessie.py\n",
    "spark/Dockerfile.dockerignore": b"*\n!spark\n!scripts/stack/nessie.py\n",
    "spark/jobs/x.py": b"x = 1\n",
    "spark/silver/y.py": b"y = 2\n",
    "scripts/stack/nessie.py": b"z = 3\n",
    "scripts/stack/other.py": b"other = 4\n",
    "README.md": b"readme\n",
}


def image_listing(files: dict[str, bytes], *paths: str) -> str:
    """What ``SOURCE_SCRIPT`` prints in an image that holds the given files under /opt/jobs."""
    return "".join(f"{blob_of(files[path])} {path}\n" for path in sorted(paths))


class ManifestSourceTest(unittest.TestCase):
    """The host has no Git history: the files that the image is compared with come from the deploy's source manifest, each one checked against its digest."""

    def source(
        self, files: dict[str, bytes] = MANIFEST_FILES, listed: dict[str, str] | None = None, on_disk: dict[str, bytes] | None = None
    ) -> tuple[str, str, str] | None:
        disk = files if on_disk is None else on_disk
        return host_mode.manifest_source(manifest_of(files) if listed is None else listed, disk.get)

    def test_the_listing_has_the_blob_id_of_each_file_that_a_copy_names_and_the_image_that_holds_those_files_is_the_tested_build(self) -> None:
        found = self.source()
        self.assertIsNotNone(found)
        tree, dockerfile, dockerignore = found or ("", "", "")
        self.assertEqual(MANIFEST_FILES["spark/Dockerfile"].decode(), dockerfile)
        self.assertEqual(MANIFEST_FILES["spark/Dockerfile.dockerignore"].decode(), dockerignore)
        self.assertIn(f"100644 blob {blob_of(MANIFEST_FILES['spark/jobs/x.py'])}\tspark/jobs/x.py\n", tree)
        self.assertNotIn("README.md", tree)
        listing = image_listing(MANIFEST_FILES, "spark/jobs/x.py", "spark/silver/y.py", "scripts/stack/nessie.py")
        self.assertIsNone(source_problem(listing, tree, dockerfile, dockerignore))
        self.assertEqual("image_source_differs", source_problem(listing.replace(blob_of(b"x = 1\n"), blob_of(b"x = 9\n")), tree, dockerfile, dockerignore))
        self.assertEqual("image_source_differs", source_problem(listing + f"{SHA_X} spark/jobs/extra.py\n", tree, dockerfile, dockerignore))
        self.assertEqual(
            "image_source_missing", source_problem(listing.replace(f"{blob_of(b'y = 2\n')} spark/silver/y.py\n", ""), tree, dockerfile, dockerignore)
        )

    def test_a_file_that_differs_from_its_manifest_digest_gives_no_listing(self) -> None:
        for path in ("spark/Dockerfile", "spark/Dockerfile.dockerignore", "spark/jobs/x.py", "scripts/stack/nessie.py"):
            with self.subTest(path=path):
                self.assertIsNone(self.source(on_disk={**MANIFEST_FILES, path: MANIFEST_FILES[path] + b"# changed\n"}))

    def test_a_file_that_is_gone_gives_no_listing_and_a_file_that_the_manifest_does_not_list_is_not_in_it_so_the_image_is_refused(self) -> None:
        listing = image_listing(MANIFEST_FILES, "spark/jobs/x.py", "spark/silver/y.py", "scripts/stack/nessie.py")
        for path in ("spark/Dockerfile", "spark/Dockerfile.dockerignore", "spark/jobs/x.py", "scripts/stack/nessie.py"):
            with self.subTest(path=path, case="gone"):
                self.assertIsNone(self.source(on_disk={name: data for name, data in MANIFEST_FILES.items() if name != path}))
            with self.subTest(path=path, case="not listed"):
                found = self.source(listed={name: digest for name, digest in manifest_of(MANIFEST_FILES).items() if name != path})
                if path.startswith("spark/Dockerfile"):
                    self.assertIsNone(found)
                else:
                    self.assertIsNotNone(found)
                    self.assertIn(
                        source_problem(listing, *(found or ("", "", ""))), ("image_source_differs", "image_source_missing", "image_source_unreadable")
                    )

    def test_a_file_that_the_docker_ignore_file_leaves_out_is_never_read_and_is_not_in_the_listing(self) -> None:
        files = {**MANIFEST_FILES, "spark/Dockerfile.dockerignore": MANIFEST_FILES["spark/Dockerfile.dockerignore"] + b"spark/silver\n"}
        read: list[str] = []

        def reading(path: str) -> bytes | None:
            read.append(path)
            return files.get(path)

        found = host_mode.manifest_source(manifest_of(files), reading)
        self.assertIsNotNone(found)
        self.assertNotIn("spark/silver/y.py", read)
        self.assertNotIn("scripts/stack/other.py", read)
        self.assertNotIn("README.md", read)
        self.assertNotIn("spark/silver/y.py", (found or ("", "", ""))[0])
        self.assertIn("spark/jobs/x.py", read)
        self.assertEqual(len(read), len(set(read)))

    def test_a_path_that_could_forge_a_listing_line_gives_no_listing(self) -> None:
        for name in ("spark/a\nb.py", "spark/a\rb.py", "spark/a\tb.py", "spark/a\x00b.py", "spark/a\x7fb.py"):
            with self.subTest(name=repr(name)):
                files = {**MANIFEST_FILES, name: b"forged = 1\n"}
                self.assertIsNone(self.source(files))

    def test_a_dockerfile_or_ignore_file_that_the_check_cannot_read_gives_no_listing(self) -> None:
        for path, text in (
            ("spark/Dockerfile", b"COPY spark/*.py /opt/jobs/spark/\n"),
            ("spark/Dockerfile", b"RUN true\n"),
            ("spark/Dockerfile", b"\xff\xfe"),
            ("spark/Dockerfile.dockerignore", b"*\n!spark/*.py\n"),
            ("spark/Dockerfile.dockerignore", b"\xff"),
        ):
            with self.subTest(path=path, text=text):
                files = {**MANIFEST_FILES, path: text}
                self.assertIsNone(self.source(files))

    def test_a_manifest_that_lists_none_of_the_copied_files_refuses_every_image(self) -> None:
        only = {"spark/Dockerfile": MANIFEST_FILES["spark/Dockerfile"], "spark/Dockerfile.dockerignore": MANIFEST_FILES["spark/Dockerfile.dockerignore"]}
        found = self.source(only)
        self.assertIsNotNone(found)
        listing = image_listing(MANIFEST_FILES, "spark/jobs/x.py", "spark/silver/y.py", "scripts/stack/nessie.py")
        self.assertEqual("image_source_unreadable", source_problem(listing, *(found or ("", "", ""))))

    def test_the_listing_of_the_real_checkout_gives_the_same_files_as_the_listing_of_git(self) -> None:
        sources = ("spark", "scripts", "features", "casefile", "replay", "tests/fixtures/vitaldb")
        done = run_command("git", ["-C", str(ROOT), "archive", "--format=tar", "HEAD", "--", *sources], timeout=120, check=True, keep_newlines=True)
        archive = os.fsencode(done.stdout)
        files: dict[str, bytes] = {}
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for member in tar:
                handle = tar.extractfile(member) if member.isreg() else None
                if handle is not None:
                    files[member.name] = handle.read()
        found = host_mode.manifest_source(manifest_of(files), files.get)
        self.assertIsNotNone(found)
        tree, dockerfile, dockerignore = found or ("", "", "")
        by_git = host_mode.image_source_files(
            committed("show", "HEAD:spark/Dockerfile"), committed("ls-tree", "-r", "HEAD"), committed("show", "HEAD:spark/Dockerfile.dockerignore")
        )
        by_manifest = host_mode.image_source_files(dockerfile, tree, dockerignore)
        self.assertEqual(174, len(by_git or {}))
        self.assertEqual(by_git, by_manifest)
        listing = "".join(f"{blob} {path}\n" for path, blob in sorted((by_manifest or {}).items()))
        self.assertIsNone(source_problem(listing, tree, dockerfile, dockerignore))


if __name__ == "__main__":
    unittest.main()
