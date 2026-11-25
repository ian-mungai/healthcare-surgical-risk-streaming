"""The owner's receipt for a verified evidence readback, and the check that teardown makes of it.

The owner's verifier signs the receipt with a private key that exists only on the owner's computer. Teardown checks
the signature against the public key in ``infra/evidence/allowed_signers`` as committed at the run's bound commit, so a receipt written by anything without
that private key is refused, whatever values it holds.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from scripts.infra.state_bootstrap import RunBinding
from scripts.ingestion.output_file import write_new_file
from scripts.process import run_command
from scripts.project_config import ROOT

SIGNER = "evidence-verifier"
NAMESPACE = "evidence-receipt"
SIGNATURE_TIMEOUT_SECONDS = 60


def trusted_signers() -> str:
    """The signer list as committed at the checked-out commit, or an empty text when that commit does not hold it. A run's files cannot choose it."""
    result = run_command("git", ["show", "HEAD:infra/evidence/allowed_signers"], cwd=ROOT, timeout=SIGNATURE_TIMEOUT_SECONDS)
    return result.stdout if result.returncode == 0 else ""


def signature_is_valid(receipt: bytes | None, signature: bytes | None, signers: str) -> bool:
    """Whether the owner's verifier key signed exactly these receipt bytes."""
    if receipt is None or signature is None or not signers.strip():
        return False
    with tempfile.TemporaryDirectory(prefix="receipt_") as scratch:
        signature_file = Path(scratch) / "receipt.sig"
        signers_file = Path(scratch) / "allowed_signers"
        write_new_file(signature_file, signature)
        write_new_file(signers_file, signers)
        arguments = ["-Y", "verify", "-f", str(signers_file), "-I", SIGNER, "-n", NAMESPACE, "-s", str(signature_file)]
        return run_command("ssh-keygen", arguments, input_text=receipt.decode(errors="replace"), timeout=SIGNATURE_TIMEOUT_SECONDS).returncode == 0


def receipt_problems(receipt: bytes | None, signed_by_owner: bool, run: RunBinding, manifest_hash: str) -> list[str]:
    """Each reason to refuse the receipt: absent, corrupt, unsigned, stale or bound to another run or commit."""
    if receipt is None:
        return ["no receipt: the evidence readback is not verified on the owner's computer"]
    try:
        record = json.loads(receipt)
    except ValueError:
        return ["the receipt is corrupt: it is not a JSON record"]
    if not isinstance(record, dict):
        return ["the receipt is corrupt: it is not a JSON record"]
    if not signed_by_owner:
        return ["the receipt is not signed by the owner's verifier"]
    problems = []
    if record.get("run_id") != run.run_id:
        problems.append("the receipt belongs to another run")
    if record.get("commit") != run.commit:
        problems.append("the receipt belongs to another commit")
    if record.get("manifest_sha256") != manifest_hash:
        problems.append("the receipt is stale: it verifies a different export manifest")
    return problems
