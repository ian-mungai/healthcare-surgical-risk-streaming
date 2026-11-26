# The image of the three Dagster services: the web server, the daemon and the code location server. Build context: this folder.
# Four more named build contexts hold what the code location needs: `stack_scripts` is the repository's scripts folder, `stack_compose` is its compose folder,
# `stack_spark` is its spark folder and `stack_fixtures` is the folder of the tracked VitalDB pin. Build with: docker build --build-context stack_scripts=<scripts>
# --build-context stack_compose=<compose> --build-context stack_spark=<spark> --build-context stack_fixtures=<tests/fixtures/vitaldb> orchestration
FROM python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/dagster/app \
    DAGSTER_HOME=/opt/dagster/instance \
    STACK_COMPOSE_DIRECTORY=/opt/dagster/stack/compose

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/app /opt/dagster/instance/storage /opt/dagster/data /opt/dagster/runs /opt/dagster/staging \
    && chown -R dagster:dagster /opt/dagster

COPY requirements.txt /opt/dagster/requirements.txt
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/dagster/requirements.txt

# The Docker command line and its Compose plugin, pinned by version and by the digest of the official image that holds them.
COPY --from=docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d \
    /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

# The AWS command line, which the S3 store of the record load runs as the program aws: the bundle of the official image, pinned by version and by the digest of its
# multi-platform index (linux/amd64 manifest sha256:7a878f76b10d19718cc25851a92a8eef6e0d5105569071e51b0480694776a8c9), linked into the path.
COPY --from=amazon/aws-cli:2.42.1@sha256:b0102d2269f87d8531d931c1a59e248101ace586886e6c077058d94c1c37b846 /usr/local/aws-cli /usr/local/aws-cli
RUN ln -s /usr/local/aws-cli/v2/current/bin/aws /usr/local/bin/aws

COPY --from=stack_scripts __init__.py aws_cli.py process.py project_config.py /opt/dagster/app/scripts/
COPY --from=stack_scripts stack/__init__.py stack/admission.py stack/groups.py stack/groups_k3s.py stack/job_workloads.py stack/limits.py stack/nessie.py stack/spark_run.py stack/spark_slot.py stack/tags.py stack/yaml_text.py \
    /opt/dagster/app/scripts/stack/
# The table checks and the record load: the ingestion package (gate, manifest, table checks, copy, Airbyte client, catalog and load),
# the pinned publisher files they read, the rules of the Spark job that the controller also holds the result to, and the tracked release pin that the manifest
# reader and the track list's pin open.
COPY --from=stack_scripts ingestion/__init__.py ingestion/airbyte_api.py ingestion/check.py ingestion/closed.py ingestion/folder.py ingestion/gate.py ingestion/landing.py \
    ingestion/manifest.py ingestion/notice.py ingestion/output_file.py ingestion/record_catalog.py ingestion/record_copy.py ingestion/record_definitions.py ingestion/record_load.py \
    ingestion/record_run.py ingestion/record_state.py ingestion/selection.py ingestion/store.py ingestion/table_checks.py ingestion/track_list.py \
    /opt/dagster/app/scripts/ingestion/
COPY --from=stack_scripts sample/__init__.py sample/candidates.py sample/publisher.py sample/split.py /opt/dagster/app/scripts/sample/
COPY --from=stack_spark __init__.py table_checks.py /opt/dagster/app/spark/
COPY --from=stack_fixtures release.json /opt/dagster/app/tests/fixtures/vitaldb/release.json
COPY --from=stack_compose compose.yml limits.yml /opt/dagster/stack/compose/
COPY --from=stack_compose services /opt/dagster/stack/compose/services

COPY dagster.yaml workspace.yaml /opt/dagster/instance/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY surgical_risk /opt/dagster/app/surgical_risk

USER 10001:10001
WORKDIR /opt/dagster/instance
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
