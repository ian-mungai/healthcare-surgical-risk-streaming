# The image of the three Dagster services: the web server, the daemon and the code location server. Build context: this folder.
# Two more named build contexts hold what the group operations of the code location need: `stack_scripts` is the repository's scripts folder and
# `stack_compose` is its compose folder. Build with: docker build --build-context stack_scripts=<scripts> --build-context stack_compose=<compose> orchestration
FROM python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/dagster/app \
    DAGSTER_HOME=/opt/dagster/instance \
    STACK_COMPOSE_DIRECTORY=/opt/dagster/stack/compose

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/app /opt/dagster/instance/storage /opt/dagster/data /opt/dagster/runs \
    && chown -R dagster:dagster /opt/dagster

COPY requirements.txt /opt/dagster/requirements.txt
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/dagster/requirements.txt

# The Docker command line and its Compose plugin, pinned by version and by the digest of the official image that holds them.
COPY --from=docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d \
    /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

COPY --from=stack_scripts __init__.py process.py project_config.py /opt/dagster/app/scripts/
COPY --from=stack_scripts stack/__init__.py stack/admission.py stack/groups.py stack/groups_k3s.py stack/limits.py stack/nessie.py stack/yaml_text.py \
    /opt/dagster/app/scripts/stack/
COPY --from=stack_compose compose.yml limits.yml /opt/dagster/stack/compose/
COPY --from=stack_compose services /opt/dagster/stack/compose/services

COPY dagster.yaml workspace.yaml /opt/dagster/instance/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY surgical_risk /opt/dagster/app/surgical_risk

USER 10001:10001
WORKDIR /opt/dagster/instance
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
