# The image of the replay service: the replay, the registration of the Avro formats and the check of what Kafka holds.
# Build context: the repository root, with replay/Dockerfile.dockerignore: `docker build --file replay/Dockerfile .`
# The image holds the checksum check's gate from scripts/ingestion, which the replay calls before it reads a case file.
FROM python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/replay/app

RUN useradd --uid 10002 --user-group --no-create-home --shell /usr/sbin/nologin replay \
    && mkdir -p /opt/replay/app /opt/replay/state /opt/replay/run \
    && chown -R replay:replay /opt/replay

COPY replay/requirements.txt /opt/replay/requirements.txt
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/replay/requirements.txt

COPY replay /opt/replay/app/replay
COPY scripts/__init__.py /opt/replay/app/scripts/__init__.py
COPY scripts/ingestion /opt/replay/app/scripts/ingestion

USER 10002:10002
WORKDIR /opt/replay
ENTRYPOINT ["python", "-m", "replay.service"]
