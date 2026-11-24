# The image of the replay service: the replay, the registration of the Avro formats and the check of what Kafka holds.
# Build context: the repository root, with replay/Dockerfile.dockerignore: `docker build --file replay/Dockerfile .`
# The image holds the checksum check's gate from scripts/ingestion, which the replay calls before it reads a case file, the modules the gate imports, the shared case-file package and
# the tracked release pin that the gate reads through ROOT, at the same paths below /opt/replay/app as in the repository.
FROM python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/replay/app

RUN useradd --uid 10003 --user-group --no-create-home --shell /usr/sbin/nologin replay \
    && mkdir -p /opt/replay/app /opt/replay/state /opt/replay/run \
    && chown -R replay:replay /opt/replay

COPY replay/requirements.txt /opt/replay/requirements.txt
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/replay/requirements.txt

COPY features/__init__.py features/sampling.py /opt/replay/app/features/
COPY casefile /opt/replay/app/casefile
COPY replay /opt/replay/app/replay
COPY scripts/__init__.py scripts/process.py scripts/project_config.py /opt/replay/app/scripts/
COPY scripts/ingestion /opt/replay/app/scripts/ingestion
COPY scripts/sample /opt/replay/app/scripts/sample
COPY tests/fixtures/vitaldb/release.json /opt/replay/app/tests/fixtures/vitaldb/release.json

USER 10003:10003
WORKDIR /opt/replay
ENTRYPOINT ["python", "-m", "replay.service"]
