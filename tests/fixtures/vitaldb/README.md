# Pinned VitalDB files

This folder pins files of the VitalDB 1.0.0 release by key, size and SHA-256. It holds no recording and no copy of a table.

- `release.json` pins the release path, the publisher's checksum list (`SHA256SUMS.txt`), the two tables (`clinical_data.csv` and `lab_data.csv`) and the publisher's track list with its row count and the digest of its rows, and carries the credit below. The SHA-256 of the checksum list is the value the pinned file had when it was read.
- `candidates.tsv` lists the candidate cases for the fixed sample: the case number, the patient number, the source key, the size in bytes, the publisher's SHA-256, whether the case has the `SNUADC/ART` waveform and the rule that chose it. `scripts/sample/candidates.py` rebuilds it from the publisher's files and the frozen patient assignment rule in `scripts/sample/split.py`.
- Check a downloaded track list against the pinned row count and digest with `.venv/bin/python -m scripts.ingestion.track_list check --file track_list.csv`.
- Rebuild or check it with `.venv/bin/python -m scripts.sample.candidates build --output tests/fixtures/vitaldb/candidates.tsv` or `... check`. Both read the public bucket without credentials.

Hand-built cases are kept in `tests/fixtures/boundary/`, never here.

## Credit

Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet. <https://doi.org/10.13026/czw8-9p62>

Source: <https://physionet.org/content/vitaldb/1.0.0/>. Licence: [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/). Changes: the publisher's files are not changed. This repository pins keys, case numbers, sizes and checksums only.
