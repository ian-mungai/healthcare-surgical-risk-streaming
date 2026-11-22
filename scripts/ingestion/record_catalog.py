"""The catalog calls of the record load, over the project's Nessie client (``scripts.stack.nessie``).

The load branch is created from the head of ``main``. A table's snapshot on a branch is the ``snapshotId`` of its Iceberg table content, read from the content
of the key ``raw_records.<table>`` at the branch's head.
"""

from __future__ import annotations

from urllib.parse import quote

from scripts.ingestion.record_definitions import MAIN_BRANCH, NAMESPACE
from scripts.stack.nessie import NessieClient, NessieError

NOT_FOUND = 404


class NessieCatalog:
    def __init__(self, client: NessieClient) -> None:
        self.client = client

    def head(self, branch: str) -> str:
        return self.client.reference(branch).hash

    def has_branch(self, branch: str) -> bool:
        try:
            self.client.reference(branch)
        except NessieError as error:
            if error.status == NOT_FOUND:
                return False
            raise
        return True

    def create_branch(self, branch: str) -> str:
        """Create the branch at the head of main and return that head."""
        return self.client.create_branch(branch, MAIN_BRANCH).hash

    def delete_branch(self, branch: str, head: str) -> None:
        self.client.delete_reference(branch, head)

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        """The snapshot of each Iceberg table of the raw namespace at the head of the branch, by table name. A key with no snapshot is not listed."""
        found: dict[str, int] = {}
        for key in self.client.entries(branch):
            namespace, _, table = key.partition(".")
            if namespace != NAMESPACE or not table or "." in table:
                continue
            answer = self.client.request("GET", f"/trees/{quote(branch, safe='')}/contents/{quote(key, safe='')}")
            content = answer.get("content") if isinstance(answer, dict) else None
            snapshot = content.get("snapshotId") if isinstance(content, dict) and content.get("type") == "ICEBERG_TABLE" else None
            if isinstance(snapshot, int) and not isinstance(snapshot, bool):
                found[table] = snapshot
        return found
