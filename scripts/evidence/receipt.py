"""The owner's receipt for a verified evidence readback, and the check that teardown makes of it.

The owner's verifier signs the receipt with a private key that exists only on the owner's computer. Teardown checks
the signature against the public key in ``infra/evidence/allowed_signers``, so a receipt written by anything without
that private key is refused, whatever values it holds.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.infra.state_bootstrap import RunBinding
from scripts.process import run_command
from scripts.project_config import ROOT

ALLOWED_SIGNERS = ROOT / "infra" / "evidence" / "allowed_signers"
SIGNER = "evidence-verifier"
NAMESPACE = "evidence-receipt"
SIGNATURE_TIMEOUT_SECONDS = 60


def signature_is_valid(receipt: Path) -> bool:
    """Whether the receipt's signature file was made by the owner's verifier key."""
    signature = receipt.with_name(receipt.name + ".sig")
    if not receipt.is_file() or not signature.is_file() or not ALLOWED_SIGNERS.is_file():
        return False
    arguments = ["-Y", "verify", "-f", str(ALLOWED_SIGNERS), "-I", SIGNER, "-n", NAMESPACE, "-s", str(signature)]
    return run_command("ssh-keygen", arguments, input_text=receipt.read_text(), timeout=SIGNATURE_TIMEOUT_SECONDS).returncode == 0


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
