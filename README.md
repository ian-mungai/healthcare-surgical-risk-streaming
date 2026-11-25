# Surgical risk streaming

A self-hosted data platform that takes real surgical monitor recordings from ingestion to reporting.

In development. The first phase builds the host and its lifecycle. It adds the repository checks, the AWS grants and the Terraform and Ansible code for the host. It also adds the deploy workflow and the commands that create, stop, start and destroy the host.

## Install

You need macOS on Apple silicon or Linux on x86-64, the two platforms that `scripts/quality/install_tools.py` pins tools for, with Python 3.14, Git, GNU Make and curl. The host commands also need the AWS CLI, OpenSSH and the Tailscale client.

```sh
make setup  # create .venv, install the pinned tools (gitleaks, Terraform 1.16.4, tflint, Trivy), the commit and commit message hooks, the working-tree privacy hook and the pre-push hook
```

`infra/versions.tf` requires Terraform 1.16.4 or a later 1.16 release, and the hosted CI job installs 1.16.4.

The host commands read their settings from `.env`. Copy `.env.example` to `.env` and set `AWS_REGION`, `AWS_PROFILE`, `AWS_ACCOUNT_ID`, `PROJECT_NAME`, `ENVIRONMENT` and `S3_BUCKET`. Git ignores `.env`. The example file also names the optional Tailscale, evidence and service group settings.

## Usage

Each check runs at the cheapest layer that can do its job, and hosted CI runs every check in full.

1. **Commit hooks, about 90 seconds:** secrets in the staged changes, credential and data files, privacy, Ruff, MyPy, the quick tests (`tests/fast`) and the commit message.
2. **Push gate, about 40 seconds for one commit (2 minutes at most), before every push to any branch** (a plain Git `pre-push` hook that `make setup` installs, `scripts/quality/push_gate.py`): it reads what Git sends and checks every commit that the remote ref in Git's input does not hold, and the whole history for a new ref, because the receiver's history is not known. It refuses a push while tracked files have uncommitted changes, then runs the secret scan of the diffs (merge diffs included) and of whole commit messages, the commit messages, the privacy scan, the changed files' own checks (Ruff, MyPy, Terraform's formatter and the quick test modules that cover the changed files; a rename or delete of a Python file runs the whole quick group) and the dependency scan when a lock file changes. One hook is shared by every worktree and refuses a push from a worktree whose gate is missing or older than the version that `make setup` installed it for (run `make setup` or rebase). A push that sends nothing, or only deletes, runs nothing. There is no full local run before a push.
3. **Hosted CI** (`.github/workflows/ci.yml`): every check in full on each push of a work branch, each pull request, the commit that a squash merge creates on `main`, every Monday and on demand. The manual-stage hooks also scan every lock file (Trivy) and every commit message of the history for secrets. A separate headroom job, outside the required check, waits for the required job and fails any job that used over 80% of its timeout, is missing from the run or has no limit. `main` moves only through a pull request that is up to date with `main` and squash-merged after the required check passes (a recorded exception to the check-first default).

Recorded limits of the push gate (owner, Nov 23 2026, "A"; neither skips a check that publishes data):

- With `diff.renames=false` in a Git configuration, Git reports a rename as a delete and an add, so the changed-file checks select fewer quick test modules. The hosted run still runs every test.
- A `make setup` run from a worktree that holds an older gate can leave a pre-push hook that calls itself. It blocks the push and never skips a check.

The privacy scan in the gate reads the tracked blobs of the pushed commit, not the working tree, so a sparse checkout cannot hide a file from it.

Terraform providers are pinned by hash in `infra/.terraform.lock.hcl` and are not vulnerability-scanned: Trivy 0.76.2 detects no package in a Terraform lock file.

Run the checks from the repository root.

```sh
make check            # run the manual-stage hooks on every file
make test-fast        # run the fast tests
make test-slow        # run the slow tests: Ansible playbook runs, local servers and stand-ins, and the planted workflow faults (on demand; the hosted run runs them too)
make headroom         # in a hosted run only: check every job of the run against its timeout (GH_TOKEN, GITHUB_REPOSITORY and GITHUB_RUN_ID are set there)
make test-guards      # commit planted faults in a temporary repository and confirm each guard refuses its own
make test-guards PARALLEL=8  # the same, as 8 shards at once in one process within the free cores and memory, with one run record (SHARD=1/8 runs one share)
make test-deadline    # run a wait that passes its deadline and confirm it fails visibly
make check-terraform  # check the Terraform code with its plan tests, tflint and Trivy
make check-ansible    # check the Ansible playbook syntax
```

Each end-to-end run writes `record.json`, `log.txt` and `retention.json` in a new folder under `runs/`, which Git ignores. Hosted CI keeps the record and retention label of each guard shard and of the deadline case as an artifact for 30 days.

Each host command runs under the project's AWS profile named in `.env`.

```sh
make host-up                      # save a plan for review, or with CONFIRM=<code> apply it and set up the host
make host-stop                    # stop the host and keep its disk
make host-start                   # start the host again
make inventory                    # list every resource the run created
make evidence-export              # pack the run's evidence with a manifest
make evidence-verify              # check the export and sign its receipt on the owner's computer
make teardown-plan                # save the destroy plan and record what may be deleted
make host-destroy CONFIRM=<code>  # destroy only after every guard passes
```

Each service belongs to the group that `compose/limits.yml` gives it. The always-on group (`supporting`) starts with the stack. Every other group is a Compose profile of the same name and starts when a job or an operator asks for it, after the admission check. Airbyte's workloads in k3s are scaled through the Kubernetes API. A group that does not fit the available memory waits and then fails with the check's four figures.

```sh
make group-up GROUP=<name>     # admit and start the group, and wait until its services are healthy
make group-down GROUP=<name>   # stop the group's services and leave every other service running
```

The shared PostgreSQL server has 100 connection slots and keeps 3 for superusers. `compose/postgres_connections.yml` gives each client (Temporal, Airbyte's pods, Apicurio, Nessie, Alloy and the three Dagster services) a maximum, and a fast test fails when the maxima and the margin pass 97 or when a tracked file does not carry the cap that its entry states. When the start of the stack fails, the failure message says "PostgreSQL connection slots are full" and shows how many slots each account holds, if that is the cause.

The commands read the compose folder from `STACK_COMPOSE_DIRECTORY` (default: this repository's `compose`).

The patient assignment rule, one split by patient for all models, is in `scripts/sample/split.py`. Rebuild the candidate cases for the fixed sample with `.venv/bin/python -m scripts.sample.candidates check`.

## Credit

The recordings come from VitalDB, an open dataset of monitor recordings from 6,388 surgical cases. Seoul National University Hospital recorded them and PhysioNet publishes them at <https://physionet.org/content/vitaldb/1.0.0/>.

Cite the dataset as: Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet. <https://doi.org/10.13026/czw8-9p62>

The publisher also asks for the original publication, Lee, HC., Park, Y., Yoon, S.B. et al. VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients. Sci Data 9, 279 (2022), <https://doi.org/10.1038/s41597-022-01411-5>, and for the standard PhysioNet citation, Pollard, T., Moody, B. E., Lehman, L., Gow, B., Fernandes, C., Xie, C., Johnson, A., Mark, R. G., & Heldt, T. (2026). PhysioNet as a global platform for biomedical research. Nature Health. <https://doi.org/10.1038/s44360-026-00096-z>

The publisher shares the dataset under the Creative Commons Attribution 4.0 licence, <https://creativecommons.org/licenses/by/4.0/>. This repository holds no recordings and changes none. It pins keys, case numbers, sizes and checksums only, in `tests/fixtures/vitaldb/`, and keeps hand-built boundary cases apart in `tests/fixtures/boundary/`.

The code is under the MIT licence in `LICENSE`.
