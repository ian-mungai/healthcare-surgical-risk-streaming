# Surgical risk streaming

A self-hosted data platform that takes real surgical monitor recordings from ingestion to reporting. It loads the recordings, streams them, builds features, gives a live warning of a blood pressure drop and scores two risks for each surgical case.

The project is in its first phase. This repository holds the repository checks so far.

## Data

The recordings come from VitalDB, an open dataset of monitor recordings from 6,388 surgical cases, at <https://physionet.org/content/vitaldb/1.0.0/>. It is shared under the Creative Commons Attribution 4.0 licence, <https://creativecommons.org/licenses/by/4.0/>. This repository holds no recordings.

## Setup

The checks need Python 3.14, Git, GNU Make and curl.

```sh
make setup        # create .venv, install the pinned tools and the commit hooks
make check        # run every hook on every file
make test-guards  # commit planted faults in a temporary repository and confirm each guard refuses its own
```

## Licence

The code is under the MIT licence in `LICENSE`.
