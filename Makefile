PYTHON := .venv/bin/python

.PHONY: setup lock check check-terraform check-ansible test-fast test-guards host-up host-stop host-start inventory evidence-export evidence-verify teardown-plan host-destroy

setup:
	python3.14 -m venv .venv
	$(PYTHON) -m pip install --require-hashes --requirement requirements.txt
	.venv/bin/pre-commit install
	$(PYTHON) -m scripts.quality.install_tools

lock:
	.venv/bin/uv pip compile requirements.in --generate-hashes --universal --python-version 3.14 --output-file requirements.txt

check:
	.venv/bin/pre-commit run --all-files --hook-stage manual

check-terraform:
	terraform fmt -check -recursive infra
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static terraform -chdir=infra init -backend=false -input=false
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static terraform -chdir=infra validate
	TF_DATA_DIR=$(CURDIR)/.tools/terraform/static terraform -chdir=infra test
	TFLINT_PLUGIN_DIR=$(CURDIR)/.tools/tflint_plugins .tools/bin/tflint --chdir=infra --recursive --config=$(CURDIR)/infra/.tflint.hcl
	.tools/bin/trivy config --cache-dir $(CURDIR)/.tools/trivy_cache --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .terraform --quiet infra

check-ansible:
	cd ansible && ANSIBLE_CONFIG=ansible.cfg ../.venv/bin/ansible-playbook --syntax-check host.yml

test-fast:
	$(PYTHON) -m unittest discover --start-directory tests/fast --top-level-directory .

test-guards:
	$(PYTHON) -m tests.e2e.commit_guards

host-up:
	$(PYTHON) -m scripts.infra.host up $(if $(CONFIRM),--confirm $(CONFIRM))

host-stop:
	$(PYTHON) -m scripts.infra.host stop

host-start:
	$(PYTHON) -m scripts.infra.host start

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
