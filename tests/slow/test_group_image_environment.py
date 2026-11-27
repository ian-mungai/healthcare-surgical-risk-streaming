"""A shell variable of a project image setting never reaches the process that Compose interpolates the image setting in, through a real ``docker`` stand-in."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.stack import groups
from scripts.stack.limits import LIMITS_FILE, load_limits

STAND_IN = f"""#!{sys.executable}
import os, sys
from pathlib import Path

here = Path(__file__).resolve().parent
with (here / "seen.log").open("a") as log:
    log.write(" ".join(sorted(name for name in os.environ if name.startswith("IMAGE_"))) + "\\n")
    log.write("kept=" + os.environ.get("KEEP_THIS_ONE", "missing") + "\\n")
"""


class ProcessBoundaryTest(unittest.TestCase):
    def test_exactly_the_three_project_image_settings_are_the_ones_that_are_kept_out(self) -> None:
        self.assertEqual({"IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"}, set(groups.PROJECT_IMAGE_SETTINGS))

    def test_the_docker_process_of_a_group_command_sees_no_shell_value_of_a_project_image_setting_and_every_other_variable_stays(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            docker = folder / "docker"
            docker.write_text(STAND_IN)
            docker.chmod(0o755)
            planted = {name: "planted/shell_value@sha256:" + "f" * 64 for name in sorted(groups.PROJECT_IMAGE_SETTINGS)}
            others = {"IMAGE_REPOSITORY": "account/repository", "IMAGE_SPARK_EXTRA": "x", "IMAGE": "y"}
            with mock.patch.dict(os.environ, planted | others | {"KEEP_THIS_ONE": "yes", "PATH": f"{folder}{os.pathsep}{os.environ['PATH']}"}):
                groups.docker(["compose", "up"], timeout=30, label="a test", env=groups.rendered_environment(load_limits(LIMITS_FILE)))
            seen = (folder / "seen.log").read_text().splitlines()
        self.assertEqual(["IMAGE_REPOSITORY IMAGE_SPARK_EXTRA", "kept=yes"], seen)


if __name__ == "__main__":
    unittest.main()
