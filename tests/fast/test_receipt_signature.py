"""The receipt's signature check, with real keys made for the test.

The check takes the exact bytes the teardown read and the signer list of the run's bound commit, so nothing is read
from a file that could change between the check and its use.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.evidence.receipt import NAMESPACE, SIGNER, signature_is_valid
from scripts.process import run_command

RECEIPT = b'{"run_id": "20260101t0000-abcdef"}\n'


def new_key(folder: Path, name: str) -> tuple[Path, str]:
    key = folder / name
    run_command("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", name, "-f", str(key)], timeout=60, check=True)
    kind, public = key.with_suffix(".pub").read_text().split()[:2]
    return key, f"{SIGNER} {kind} {public}\n"


def sign(key: Path, content: bytes, folder: Path) -> bytes:
    target = folder / f"{key.name}_receipt.json"
    target.write_bytes(content)
    run_command("ssh-keygen", ["-Y", "sign", "-f", str(key), "-n", NAMESPACE, str(target)], timeout=60, check=True)
    return target.with_name(target.name + ".sig").read_bytes()


class SignatureTest(unittest.TestCase):
    folder: Path
    owner_key: Path
    other_key: Path
    signers: str
    signature: bytes

    @classmethod
    def setUpClass(cls) -> None:
        cls.folder = Path(tempfile.mkdtemp(prefix="signature_"))
        cls.owner_key, cls.signers = new_key(cls.folder, "owner")
        cls.other_key, _ = new_key(cls.folder, "other")
        cls.signature = sign(cls.owner_key, RECEIPT, cls.folder)

    def test_owner_signature_over_the_same_bytes_passes(self) -> None:
        self.assertTrue(signature_is_valid(RECEIPT, self.signature, self.signers))

    def test_changed_bytes_are_refused(self) -> None:
        self.assertFalse(signature_is_valid(RECEIPT.replace(b"abcdef", b"abcdee"), self.signature, self.signers))

    def test_signature_by_another_key_is_refused(self) -> None:
        self.assertFalse(signature_is_valid(RECEIPT, sign(self.other_key, RECEIPT, self.folder), self.signers))

    def test_missing_signature_or_signer_list_is_refused(self) -> None:
        self.assertFalse(signature_is_valid(RECEIPT, None, self.signers))
        self.assertFalse(signature_is_valid(RECEIPT, self.signature, ""))
        self.assertFalse(signature_is_valid(None, self.signature, self.signers))


if __name__ == "__main__":
    unittest.main()
