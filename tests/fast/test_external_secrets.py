"""The list of external secrets: its form, the named form checks and the rule that each path is read by its consumer's policy alone."""

from __future__ import annotations

import unittest
from types import ModuleType

from scripts.project_config import ROOT
from tests.fast.ansible_paths import shared_module

LIST_FILE = ROOT / "compose" / "external_secrets.txt"
TOKEN = "dckr_pat_" + "a1B2-c3D4_e5F6g7H8i9J0"


def library() -> ModuleType:
    return shared_module("external_secrets")


class ListFileTest(unittest.TestCase):
    def test_the_first_line_declares_the_docker_hub_read_token_for_the_deploy_consumer(self) -> None:
        first = library().parse_external(LIST_FILE.read_text())[0]
        self.assertEqual(
            ("dockerhub_read", "deploy", "secret/data/external/dockerhub_read", "dockerhub_token"), (first.name, first.consumer, first.path, first.check)
        )

    def test_the_file_holds_no_value_of_a_declared_form(self) -> None:
        text = LIST_FILE.read_text()
        for word in text.split():
            self.assertFalse(any(check.fullmatch(word) for check in library().FORM_CHECKS.values()), word)

    def test_every_consumer_is_a_listed_service_so_its_policy_and_its_login_exist(self) -> None:
        services = shared_module("secret_files").parse_names((ROOT / "compose" / "openbao_services.txt").read_text(), "service")
        for item in library().parse_external(LIST_FILE.read_text()):
            self.assertIn(item.consumer, services)


class ParseTest(unittest.TestCase):
    def refused(self, text: str) -> str:
        with self.assertRaises(library().ExternalSecretError) as caught:
            library().parse_external(text)
        return str(caught.exception)

    def test_comments_and_blank_lines_are_ignored_and_each_line_has_four_words(self) -> None:
        found = library().parse_external("# one\n\nalpha deploy secret/data/external/alpha dockerhub_token  # why\n")
        self.assertEqual(["alpha"], [item.name for item in found])

    def test_a_path_other_than_the_one_under_the_secrets_external_folder_is_refused(self) -> None:
        for path in ("secret/data/external/other", "secret/data/services/alpha", "secret/data/external/alpha/x", "secret/data/external/"):
            with self.subTest(path=path):
                self.assertIn("path", self.refused(f"alpha deploy {path} dockerhub_token\n"))

    def test_an_unknown_check_a_short_line_a_long_line_a_bad_name_a_repeat_and_an_empty_list_are_refused(self) -> None:
        self.assertIn("check", self.refused("alpha deploy secret/data/external/alpha nothing\n"))
        self.assertIn("four", self.refused("alpha deploy secret/data/external/alpha\n"))
        self.assertIn("four", self.refused("alpha deploy secret/data/external/alpha dockerhub_token extra\n"))
        self.assertIn("name", self.refused("Alpha deploy secret/data/external/Alpha dockerhub_token\n"))
        line = "alpha deploy secret/data/external/alpha dockerhub_token\n"
        self.assertIn("more than once", self.refused(line + line))
        self.assertIn("no external secret", self.refused("# nothing\n"))


class MalformedRowTest(unittest.TestCase):
    """A row that cannot be used is reported by its line number alone: any word of it can be a secret that was pasted into the list by mistake."""

    MARKER = "dckr_pat_" + "MarkerThatMustNotLeak0123"

    def refused(self, text: str) -> str:
        with self.assertRaises(library().ExternalSecretError) as caught:
            library().parse_external(text)
        return str(caught.exception)

    def test_each_kind_of_malformed_row_is_named_by_its_line_and_holds_no_word_of_the_row(self) -> None:
        marker = self.MARKER
        rows = {
            "one word": f"{marker}\n",
            "three words": f"alpha deploy {marker}\n",
            "five words": f"alpha deploy secret/data/external/alpha dockerhub_token {marker}\n",
            "a name that is not in the form": f"{marker.upper()} deploy secret/data/external/alpha dockerhub_token\n",
            "a consumer that is not in the form": f"alpha {marker.upper()} secret/data/external/alpha dockerhub_token\n",
            "a path that is not the name's path": f"alpha deploy {marker} dockerhub_token\n",
            "a check that is not named": f"alpha deploy secret/data/external/alpha {marker}\n",
        }
        for label, row in rows.items():
            with self.subTest(row=label):
                message = self.refused("# comment\n\n" + row)
                self.assertIn("line 3", message)
                self.assertNotIn(marker, message)
                self.assertNotIn(marker.upper(), message)

    def test_a_repeated_name_is_named_by_the_line_of_the_second_row(self) -> None:
        line = "alpha deploy secret/data/external/alpha dockerhub_token\n"
        self.assertIn("line 2", self.refused(line + line))


class FormCheckTest(unittest.TestCase):
    def accepts(self, value: str) -> bool:
        return bool(library().form_ok("dockerhub_token", value))

    def test_the_prefix_then_20_to_100_characters_of_the_set_is_accepted(self) -> None:
        self.assertTrue(self.accepts(TOKEN))
        self.assertTrue(self.accepts("dckr_pat_" + "a" * 20))
        self.assertTrue(self.accepts("dckr_pat_" + "a" * 100))
        self.assertTrue(self.accepts("dckr_pat_" + "-_" * 10))

    def test_a_wrong_prefix_a_short_or_long_body_and_any_other_character_are_refused(self) -> None:
        for value in (
            "",
            "dckr_pat_",
            "dckr_pat_" + "a" * 19,
            "dckr_pat_" + "a" * 101,
            "dckr_pat" + "a" * 30,
            "xdckr_pat_" + "a" * 30,
            "DCKR_PAT_" + "a" * 30,
            TOKEN + "\n",
            TOKEN + " ",
            " " + TOKEN,
            TOKEN[:15] + " " + TOKEN[15:],
            TOKEN[:15] + "\n" + TOKEN[15:],
            TOKEN + "é",
            TOKEN + "!",
        ):
            with self.subTest(value=repr(value[:20])):
                self.assertFalse(self.accepts(value))

    def test_an_unknown_check_name_is_refused_not_passed(self) -> None:
        with self.assertRaises(library().ExternalSecretError):
            library().form_ok("nothing", TOKEN)

    def test_the_digest_is_the_sha_256_of_the_value_in_utf_8(self) -> None:
        self.assertEqual("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", library().value_digest("abc"))


if __name__ == "__main__":
    unittest.main()
