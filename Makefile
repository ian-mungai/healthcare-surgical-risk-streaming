PYTHON := .venv/bin/python
override TERRAFORM := .tools/bin/terraform

.PHONY: images-push trivy-db setup lock lock-orchestration lock-replay lock-spark check headroom check-terraform check-ansible test-fast test-slow test-guards test-deadline host-up host-stop host-start host-runners-place storage-up inventory evidence-export evidence-verify teardown-plan host-destroy group-up group-down

setup:
	python3.14 -m venv .venv
	$(PYTHON) -m pip install --require-hashes --requirement requirements.txt
	.venv/bin/pre-commit install
	$(PYTHON) -m scripts.quality.install_tools

lock:
	.venv/bin/uv pip compile requirements.in --generate-hashes --universal --python-version 3.14 --output-file requirements.txt

lock-orchestration:
	.venv/bin/uv pip compile orchestration/requirements.in --generate-hashes --python-platform x86_64-unknown-linux-gnu --python-version 3.14 --output-file orchestration/requirements.txt

lock-replay:
	.venv/bin/uv pip compile replay/requirements.in --generate-hashes --python-platform x86_64-unknown-linux-gnu --python-version 3.14 --output-file replay/requirements.txt

lock-spark:
	.venv/bin/uv pip compile spark/requirements.in --generate-hashes --python-platform x86_64-unknown-linux-gnu --python-version 3.14 --output-file spark/requirements.txt

trivy-db:
	$(PYTHON) -m scripts.images.trivy_scan

images-push:
	$(PYTHON) -m scripts.images.publish

check:
	.venv/bin/pre-commit run --all-files --hook-stage manual

headroom:
	$(PYTHON) -m scripts.quality.headroom

check-terraform:
	@test -f $(TERRAFORM) -a -x $(TERRAFORM) || { echo "the pinned Terraform $(TERRAFORM) is missing or not an executable file; run: make setup" >&2; exit 1; }
	$(TERRAFORM) fmt -check -recursive infra
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static $(TERRAFORM) -chdir=infra init -backend=false -input=false
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static $(TERRAFORM) -chdir=infra validate
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static $(TERRAFORM) -chdir=infra test
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static_storage $(TERRAFORM) -chdir=infra/storage init -backend=false -input=false
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static_storage $(TERRAFORM) -chdir=infra/storage validate
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static_storage $(TERRAFORM) -chdir=infra/storage test
	TFLINT_PLUGIN_DIR=$(CURDIR)/.tools/tflint_plugins .tools/bin/tflint --chdir=infra --recursive --config=$(CURDIR)/infra/.tflint.hcl
	.tools/bin/trivy config --cache-dir $(CURDIR)/.tools/trivy_embedded_cache --skip-check-update --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .terraform --quiet infra

check-ansible:
	cd ansible && ANSIBLE_CONFIG=ansible.cfg ../.venv/bin/ansible-playbook --syntax-check host.yml stack.yml storage.yml connection.yml

test-fast:
	$(PYTHON) -m unittest discover --start-directory tests/fast --top-level-directory .

test-slow:
	$(PYTHON) -m unittest discover --start-directory tests/slow --top-level-directory .

test-guards:
	$(PYTHON) -m tests.e2e.commit_guards $(if $(SHARD),--shard $(SHARD)) $(if $(PARALLEL),--parallel $(PARALLEL))

test-deadline:
	$(PYTHON) -m tests.e2e.deadline

host-up:
	$(PYTHON) -m scripts.infra.host up $(if $(CONFIRM),--confirm $(CONFIRM))

storage-up:
	$(PYTHON) -m scripts.infra.storage up $(if $(CONFIRM),--confirm $(CONFIRM))

host-stop:
	$(PYTHON) -m scripts.infra.host stop

host-start:
	$(PYTHON) -m scripts.infra.host start

# The text of RUNNERS reaches the program raw, in HOST_RUNNER_NAMES, and is split there on white space and checked there: Make expands no $ in it ($(value RUNNERS)
# is the text as typed, and a := variable is not expanded again), and no shell globs, splits or expands a name. RUNNERS itself is not exported, because Make would expand it.
HOST_RUNNER_NAMES := $(value RUNNERS)
export HOST_RUNNER_NAMES
unexport RUNNERS
host-runners-place:
	$(if $(value RUNNERS),,$(error name the runner files: make host-runners-place RUNNERS="tests/e2e/silver_signals.py ..."))
	$(PYTHON) -m scripts.infra.runner_place --names-from-environment

inventory:
	$(PYTHON) -m scripts.infra.inventory

evidence-export:
	$(PYTHON) -m scripts.evidence.export

evidence-verify:
	$(PYTHON) -m scripts.evidence.verify

teardown-plan:
	$(PYTHON) -m scripts.infra.teardown plan

host-destroy:
	$(PYTHON) -m scripts.infra.teardown destroy --confirm "$(CONFIRM)"

group-up:
	$(if $(GROUP),,$(error name the group: make group-up GROUP=<name>))
	$(PYTHON) -m scripts.stack.groups up $(GROUP)

group-down:
	$(if $(GROUP),,$(error name the group: make group-down GROUP=<name>))
	$(PYTHON) -m scripts.stack.groups down $(GROUP)
