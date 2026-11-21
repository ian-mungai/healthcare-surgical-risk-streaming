# Surgical risk streaming

A self-hosted data platform that takes real surgical monitor recordings from ingestion to reporting. It loads the recordings, streams them, builds features, gives a live warning of a blood pressure drop and scores two risks for each surgical case.

The project is in its first phase, which builds the host and its lifecycle. So far this repository holds the repository checks, the AWS grants, the Terraform and Ansible code for the host, the deploy workflow and the commands that create, stop, start and destroy the host.

## Data

The recordings come from VitalDB, an open dataset of monitor recordings from 6,388 surgical cases, at <https://physionet.org/content/vitaldb/1.0.0/>. It is shared under the Creative Commons Attribution 4.0 licence, <https://creativecommons.org/licenses/by/4.0/>. This repository holds no recordings.

Credit: Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet. <https://doi.org/10.13026/czw8-9p62>. Changes: the publisher's files are not changed. The repository pins keys, case numbers, sizes and checksums only, in `tests/fixtures/vitaldb/`, and keeps hand-built boundary cases apart in `tests/fixtures/boundary/`.

The patient assignment rule, one split by patient for all models, is in `scripts/sample/split.py`. Rebuild the candidate cases for the fixed sample with `.venv/bin/python -m scripts.sample.candidates check`.

## Setup

The checks need Python 3.14, Git, GNU Make and curl.

```sh
make setup        # create .venv, install the pinned tools, the commit hooks and the working-tree privacy hook
make check        # run every hook on every file
make test-guards  # commit planted faults in a temporary repository and confirm each guard refuses its own
```

## Host commands

Each command runs under the project's AWS profile named in `.env`, which `.env.example` describes.

```sh
make host-up               # save a plan for review; with CONFIRM=<code>, apply it and set up the host
make host-stop             # stop the host and keep its disk
make host-start            # start the host again
make inventory             # list every resource the run created
make evidence-export       # pack the run's evidence with a manifest
make evidence-verify       # check the export and sign its receipt, on the owner's computer
make teardown-plan         # save the destroy plan and record what may be deleted
make host-destroy CONFIRM=<code>  # destroy only after every guard passes
```

## Licence

The code is under the MIT licence in `LICENSE`.
