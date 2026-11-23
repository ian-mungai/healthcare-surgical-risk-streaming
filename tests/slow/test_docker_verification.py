"""The Docker check proves the effective log rotation: the driver, the settings in the file and that Docker started after the file was written.

The role registers the running driver, the content and modification time of ``/etc/docker/daemon.json`` and the time
Docker last became active. The check runs here through the real ``ansible-playbook`` with the values a host would
give, so these cases cover a stale file, a wrong setting and a Docker that never read the file.
"""

from __future__ import annotations

import base64
import json
import unittest

from tests.slow.ansible_support import run_task_file

WRITTEN = 1_000
GOOD_CONFIGURATION = {"log-driver": "json-file", "log-opts": {"max-size": "50m", "max-file": "5"}}
MESSAGE = "Docker's log rotation is not in effect"


def verdict(
    configuration: object = None, *, driver: str = "json-file", started: int = WRITTEN + 5, written: int = WRITTEN, size: str = "50m", files: str = "5"
) -> tuple[int, str]:
    content = json.dumps(GOOD_CONFIGURATION if configuration is None else configuration)
    result = run_task_file(
        "docker",
        "verify_log_rotation.yml",
        {
            "docker_log_max_size": size,
            "docker_log_max_file": files,
            "docker_logging_driver": {"stdout": driver},
            "docker_daemon_configuration": {"content": base64.b64encode(content.encode()).decode()},
            "docker_daemon_file": {"stat": {"mtime": float(written) + 0.25}},
            "docker_started": {"stdout": f"@{started}\n"},
        },
    )
    return result.returncode, result.stdout + result.stderr


class LogRotationTest(unittest.TestCase):
    def test_the_expected_file_and_a_later_start_pass(self) -> None:
        code, output = verdict()
        self.assertEqual(0, code, output)

    def test_a_start_in_the_same_second_as_the_write_passes(self) -> None:
        code, output = verdict(started=WRITTEN)
        self.assertEqual(0, code, output)

    def test_a_docker_that_started_before_the_file_was_written_is_refused(self) -> None:
        code, output = verdict(started=WRITTEN - 1)
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_different_size_limit_in_the_file_is_refused(self) -> None:
        code, output = verdict(GOOD_CONFIGURATION | {"log-opts": {"max-size": "500m", "max-file": "5"}})
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_different_file_count_in_the_file_is_refused(self) -> None:
        code, output = verdict(GOOD_CONFIGURATION | {"log-opts": {"max-size": "50m", "max-file": "50"}})
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_file_without_rotation_settings_is_refused(self) -> None:
        code, output = verdict({"log-driver": "json-file"})
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_running_driver_other_than_json_file_is_refused(self) -> None:
        code, output = verdict(driver="local")
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_the_pinned_settings_decide_not_fixed_values(self) -> None:
        code, output = verdict(GOOD_CONFIGURATION | {"log-opts": {"max-size": "10m", "max-file": "3"}}, size="10m", files="3")
        self.assertEqual(0, code, output)
        code, output = verdict(size="10m", files="3")
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)


if __name__ == "__main__":
    unittest.main()
