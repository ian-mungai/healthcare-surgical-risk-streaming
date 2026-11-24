"""The shared Git config of the project is the same at the end of the quick run as at its start.

Every worktree reads that file. A test that ran ``git init`` with ``GIT_DIR`` inherited from a hook once set ``core.bare`` in it, and the main checkout lost
its working tree (Nov 23 2026). The file name sorts last, so this test runs after every other quick test of the run.
"""

from __future__ import annotations

import unittest

from tests.fast.git_fixture import SHARED_CORE_AT_START, shared_core_settings


class SharedGitConfigTest(unittest.TestCase):
    def test_the_core_settings_did_not_change_during_the_run(self) -> None:
        self.assertEqual(shared_core_settings(), SHARED_CORE_AT_START)

    def test_the_repository_is_not_bare(self) -> None:
        self.assertNotIn("core.bare true", shared_core_settings())


if __name__ == "__main__":
    unittest.main()
