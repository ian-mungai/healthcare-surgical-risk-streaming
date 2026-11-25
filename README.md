# Surgical risk streaming

A self-hosted data platform that takes real surgical monitor recordings from ingestion to reporting.

In development. The first phase builds the host and its lifecycle. It adds the repository checks, the AWS grants and the Terraform and Ansible code for the host. It also adds the deploy workflow and the commands that create, stop, start and destroy the host.

## Install

You need macOS on Apple silicon or Linux on x86-64, the two platforms that `scripts/quality/install_tools.py` pins tools for, with Python 3.14, Git, GNU Make and curl. The host commands also need the AWS CLI, OpenSSH and the Tailscale client. Reading AIStor's licence from this computer's OpenBao also needs the macOS Keychain (the `security` command): without it the deploy skips AIStor with a notice and goes on (see the AIStor licence step below).

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
make storage-up                   # save a plan for the AIStor bucket for review, or with CONFIRM=<code> apply it and check the bucket's versioning
```

AIStor keeps its root user and password only in root-only files on the host (mode 0400, made once at the first deploy, given to the server through `MINIO_ROOT_USER_FILE` and `MINIO_ROOT_PASSWORD_FILE`). Nothing uses them for the project's work, and the deploy uses them on the FIRST bootstrap of a host only. That bootstrap reads the pair inside the AIStor container to make two accounts: the project user (only when the server says it does not exist) with its policy, exactly the S3 calls that the pinned Terraform provider makes for the bucket and its versioning, on the one bucket, with no object action, no bucket-policy write and no version delete; and a read-only audit identity (`project_audit`) whose one policy allows only the admin reads `admin:GetUser` (a user's policies, status and groups) and `admin:GetPolicy` (a policy's document) and no S3 call. Each account holds its policy and no other policy and no group; a policy or group that someone added is removed before a disabled user is enabled. The two passwords are root-only files on the host. OpenBao is given each pair only after that pair has signed in (`secret/services/aistor/aistor` for the project user, which only the `aistor` role reads, and `secret/services/aistor/audit` for the audit identity, which only the deploy's admin role reads), so a password file that was lost and made again never replaces a working value. The bootstrap ends by writing a root-only marker file on the host (`/opt/platform/compose/aistor.bootstrap`, mode 0400, beside `aistor.state`), and a run that fails before that is the first bootstrap again.

Every later deploy makes no call with the root pair and no write. It reads the live state as the audit identity and the project user with the pairs that OpenBao keeps, and checks that the project user is enabled, holds exactly the project policy by name and no other policy, is in no group (it reads the user's raw admin answer, `GET /minio/admin/v3/user-info`, with the AIStor container's curl, because `mc admin user info` also reads each group's description, which needs `admin:GetGroup`, and the audit identity holds only `admin:GetUser` and `admin:GetPolicy`; an added group is named in the message), and that the project policy's document equals the desired one; then it lists the project's bucket as the project user. A difference, a refused sign-in or a missing OpenBao entry fails the deploy with a fixed message that names what differs and changes nothing. An `AccessDenied` answer (an offline or unlicensed server gives it too) is a failure and never a reason to use root. The root pair is used again only when the owner approves it for one run, by running `make host-up CONFIRM=<code> AISTOR_REPAIR=1` (the launcher's flag `--aistor-repair`, which passes the extra variable `aistor_repair_once` as true to the AIStor playbook of that one run and to no other playbook; every other run passes it as false, and an extra variable outranks any inventory value of the same name, so no file can turn the repair on; `make` passes the flag only for a literal `AISTOR_REPAIR=1` on its own command line, never for another value or an exported variable). No file holds the approval, so the next `make host-up` is a plain run again and a repair that was done never repeats. The approval is not an inventory variable, and the old inventory variable `compose_stack_aistor_root_repair` is refused whatever its value: the AIStor step stops with a message that names it and asks for it to be removed. That repair removes what is not the project's, restores what is missing and, for an account that exists and that neither OpenBao's sign-in nor the password file signs in as, sets the password file's value on it. A host that was bootstrapped before the marker existed gets it with one root run that changes nothing else. If the host's AIStor data is replaced while the marker stays, the next deploy fails closed (no project user) and needs the same approval (`AISTOR_REPAIR=1`). `make storage-up` and every other storage client sign in as the project user, which is also Terraform's identity (bucket-only, never the audit identity); workloads use STS. `make storage-up` first refuses a provider lock (`infra/storage/.terraform.lock.hcl`) that holds no registry hashes and names the owner's step (`.tools/bin/terraform -chdir=infra/storage providers lock -platform=<this computer's platform> -platform=linux_amd64`, then commit the file), and it refuses a sign-in in OpenBao for any account but the project user before Terraform starts.

AIStor's licence (the Free licence from MinIO) is kept in this computer's OpenBao at `secret/aistor_licence`, field `value`. `make host-up` reads it with a token of its own from the Keychain item `openbao-aistor-licence-token`, never with the owner's full token (`openbao-owner-token`), and places it on the host as one root-only file (mode 0400, the file that the server reads through `--license`, as the AIStor container install guide shows) through a script that sudo does not record; it is never printed, logged or put in an Ansible variable. The token's OpenBao policy `aistor-licence-read` allows `read` on `secret/data/aistor_licence` and nothing else.

The owner makes that token once, in a separate terminal so no value reaches a chat (`references/local_machine.md`, Local secrets). The policy and the token need administration rights that the owner's token (`owner-secrets`) does not have, so first generate a root token with the unseal key as item 3 of that section says (`bao operator generate-root`) and revoke it at the end of these steps. The token is made with `-orphan` because a token that is made as a child of the administration token is revoked with it by `bao token revoke -self` ([token hierarchies and orphan tokens](https://openbao.org/docs/concepts/tokens/#token-hierarchies-and-orphan-tokens)); `-orphan` needs the root token's rights. Then run, in this order (the token is never printed: it goes to the clipboard, then into the Keychain at `security`'s own hidden prompt, and then the clipboard is cleared):

```zsh
export BAO_ADDR=http://127.0.0.1:8200
read -rs "BAO_TOKEN?Administration token: " && export BAO_TOKEN; echo
bao policy write aistor-licence-read - <<'POLICY'
path "secret/data/aistor_licence" { capabilities = ["read"] }
POLICY
bao token create -field=token -orphan -policy=aistor-licence-read -no-default-policy -period=768h | pbcopy
bao token revoke -self; unset BAO_TOKEN
security add-generic-password -a "$USER" -s openbao-aistor-licence-token -U -w   # paste at the hidden prompt, twice
pbcopy < /dev/null
```

Save the licence itself with the command in `references/local_machine.md` (item 5), with the name `aistor_licence`. Then check, after the administration token is revoked, that the saved token still works. The command reads the licence path with the token from the Keychain item, in the environment of that one command, and sends the value to `/dev/null`, so nothing is printed. An empty token stops the command before `bao` runs, because `bao` would use its token helper file instead. A 403 answer means the token or its policy is wrong (make the token again from `bao policy write`), and a 404 answer means that the licence is not saved yet:

```zsh
export BAO_ADDR=http://127.0.0.1:8200
token="$(security find-generic-password -a "$USER" -s openbao-aistor-licence-token -w)" && [[ -n "$token" ]] && BAO_TOKEN="$token" bao read secret/data/aistor_licence > /dev/null && echo "The licence token reads the licence path."; unset token
```

A periodic token expires after 768 hours without a renewal, and the unseal job renews only `openbao-agent-token` and `openbao-owner-token`: when the launcher says that OpenBao refused the read (HTTP 403), run the steps again from `bao policy write` with a new administration token.

A computer without that Keychain item, or without the `security` command (Linux), has no licence source. A licence that OpenBao does not hold is the same case for the deploy: nothing is sent, and the deploy goes on with everything else (Airbyte, the connection step and the rest). `make host-up` tells the AIStor playbook only one non-secret fact, `placed` (the licence is on the host) or `absent`. With `placed`, a licence file that is missing on the host fails the AIStor step. With `absent` and no licence file on the host, the playbook skips AIStor and writes `skipped` to `/opt/platform/compose/aistor.state`, and only then does `make host-up` read that record and print the notice (it names this step when the cause is a missing token); with `absent` and a licence file already on the host, AIStor starts with that file and the notice says so. Save the licence, make the token if it is missing and run `make host-up` again: AIStor starts and the record says `started`. `make storage-up` reads that record first and, for a skipped AIStor, names this step.

An interrupted placement can leave a second `0400` file, `.licence.<8 hex>.new`, that holds a copy of the licence. The placement script removes it when the SSH session drops or the script is told to stop (a hangup or a termination), and every deploy's licence check removes any that is left by a kill that cannot be caught, before the licence is read.

Each service belongs to the group that `compose/limits.yml` gives it. The always-on group (`supporting`) starts with the stack. Every other group is a Compose profile of the same name and starts when a job or an operator asks for it, after the admission check. Airbyte's workloads in k3s are scaled through the Kubernetes API. A group that does not fit the available memory waits and then fails with the check's four figures.

```sh
make group-up GROUP=<name>     # admit and start the group, and wait until its services are healthy
make group-down GROUP=<name>   # stop the group's services and leave every other service running
```

The commands read the compose folder from `STACK_COMPOSE_DIRECTORY` (default: this repository's `compose`).

The patient assignment rule, one split by patient for all models, is in `scripts/sample/split.py`. Rebuild the candidate cases for the fixed sample with `.venv/bin/python -m scripts.sample.candidates check`.

## Credit

The recordings come from VitalDB, an open dataset of monitor recordings from 6,388 surgical cases. Seoul National University Hospital recorded them and PhysioNet publishes them at <https://physionet.org/content/vitaldb/1.0.0/>.

Cite the dataset as: Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet. <https://doi.org/10.13026/czw8-9p62>

The publisher also asks for the original publication, Lee, HC., Park, Y., Yoon, S.B. et al. VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients. Sci Data 9, 279 (2022), <https://doi.org/10.1038/s41597-022-01411-5>, and for the standard PhysioNet citation, Pollard, T., Moody, B. E., Lehman, L., Gow, B., Fernandes, C., Xie, C., Johnson, A., Mark, R. G., & Heldt, T. (2026). PhysioNet as a global platform for biomedical research. Nature Health. <https://doi.org/10.1038/s44360-026-00096-z>

The publisher shares the dataset under the Creative Commons Attribution 4.0 licence, <https://creativecommons.org/licenses/by/4.0/>. This repository holds no recordings and changes none. It pins keys, case numbers, sizes and checksums only, in `tests/fixtures/vitaldb/`, and keeps hand-built boundary cases apart in `tests/fixtures/boundary/`.

The code is under the MIT licence in `LICENSE`.
