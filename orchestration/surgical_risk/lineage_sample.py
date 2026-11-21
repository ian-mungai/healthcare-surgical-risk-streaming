"""The sample asset that proves the lineage graph: one input, one asset and one asset check, with no data of the project.

Dagster's asset graph for ``lineage_proof_output`` must show its input ``lineage_proof_input`` and its check ``lineage_proof_output_has_rows``. The values are
three fixed numbers, so the sample builds no table and reads no file.
"""

from dagster import AssetCheckResult, AssetSelection, Definitions, asset, asset_check, define_asset_job

GROUP = "lineage_proof"


@asset(group_name=GROUP)
def lineage_proof_input() -> list[int]:
    return [1, 2, 3]


@asset(group_name=GROUP)
def lineage_proof_output(lineage_proof_input: list[int]) -> list[int]:
    return [value * 2 for value in lineage_proof_input]


@asset_check(asset=lineage_proof_output)
def lineage_proof_output_has_rows(lineage_proof_output: list[int]) -> AssetCheckResult:
    return AssetCheckResult(passed=len(lineage_proof_output) > 0, metadata={"rows": len(lineage_proof_output)})


lineage_proof_job = define_asset_job(
    "lineage_proof_job",
    selection=AssetSelection.assets(lineage_proof_input, lineage_proof_output) | AssetSelection.checks_for_assets(lineage_proof_output),
)

defs = Definitions(assets=[lineage_proof_input, lineage_proof_output], asset_checks=[lineage_proof_output_has_rows], jobs=[lineage_proof_job])
