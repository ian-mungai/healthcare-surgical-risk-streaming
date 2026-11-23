"""Record the service ID of every resource this run created.

Run from the repository root under the project profile in ``.env``:

``.venv/bin/python -m scripts.infra.inventory``

It reads the run's Terraform state from its bound backend and writes ``runs/<RUN_ID>/inventory.json``: one entry for
each resource that an applied plan of this run created, with its address, type and service ID, one for the project's bucket
when this run created it, one for the host's Tailscale device, the managed resources this run did not create and the items the owner authorized to retain.
Resources without tags are listed like any other, because the list comes from the creation record and the state.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from scripts.infra.created import DEVICE_FILE, device_entries, run_inventory
from scripts.infra.state_bootstrap import DEFAULT_BINDING
from scripts.infra.terraform_wrapper import WrapperError, load_run, state_json
from scripts.ingestion.closed import QuietParser
from scripts.project_config import ROOT, ConfigurationError, load_configuration


def write_inventory(binding: Path) -> Path:
    configuration = load_configuration()
    run = load_run(binding)
    folder = ROOT / "runs" / run.run_id
    folder.mkdir(parents=True, exist_ok=True)
    retained_file = folder / "retained.json"
    retained = json.loads(retained_file.read_text()) if retained_file.is_file() else []
    created_file = folder / "created.json"
    recorded = json.loads(created_file.read_text()) if created_file.is_file() else []
    created, not_created = run_inventory(recorded, state_json(run, configuration), run)
    device_file = folder / DEVICE_FILE
    created += device_entries(created, json.loads(device_file.read_text()) if device_file.is_file() else None)
    inventory = {
        "run_id": run.run_id,
        "commit": run.commit,
        "created": created,
        "not_created_by_this_run": not_created,
        "authorized_retained": retained,
    }
    target = folder / "inventory.json"
    target.write_text(json.dumps(inventory, indent=2) + "\n")
    return target


def main() -> int:
    """Write the inventory, print its size and return a non-zero code on a failure."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    try:
        target = write_inventory(parser.parse_args().binding)
    except (ConfigurationError, WrapperError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    inventory = json.loads(target.read_text())
    counts = f"{len(inventory['created'])} created resources and {len(inventory['authorized_retained'])} retained items"
    sys.stdout.write(f"run {inventory['run_id']}: {counts} recorded in {target.relative_to(ROOT)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
