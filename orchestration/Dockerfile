# The image of the three Dagster services: the web server, the daemon and the code location server. Build context: this folder.
FROM python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/dagster/app \
    DAGSTER_HOME=/opt/dagster/instance

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/app /opt/dagster/instance/storage /opt/dagster/data \
    && chown -R dagster:dagster /opt/dagster

COPY requirements.txt /opt/dagster/requirements.txt
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/dagster/requirements.txt

COPY dagster.yaml workspace.yaml /opt/dagster/instance/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY surgical_risk /opt/dagster/app/surgical_risk

USER 10001:10001
WORKDIR /opt/dagster/instance
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
