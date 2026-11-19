PYTHON := .venv/bin/python

.PHONY: setup lock check check-terraform test-fast test-guards

setup:
	python3.14 -m venv .venv
	$(PYTHON) -m pip install --require-hashes --requirement requirements.txt
	$(PYTHON) -m scripts.quality.install_tools
	.venv/bin/pre-commit install

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

test-fast:
	$(PYTHON) -m unittest discover --start-directory tests/fast --top-level-directory .

test-guards:
	$(PYTHON) -m tests.e2e.commit_guards
