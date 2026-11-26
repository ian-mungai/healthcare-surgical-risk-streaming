# The image of the replay service: the replay, the registration of the Avro formats and the check of what Kafka holds.
# Build context: the repository root, with replay/Dockerfile.dockerignore: `docker build --file replay/Dockerfile .`
# The image holds the checksum check's gate from scripts/ingestion, which the replay calls before it reads a case file, the modules the gate imports, the shared case-file package and
# the tracked release pin that the gate reads through ROOT, at the same paths below /opt/replay/app as in the repository.
FROM python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/replay/app

# Debian's security and point-release updates that the base image does not hold yet, from one dated snapshot of both archives (snapshot.debian.org), so that a
# rebuild installs the same packages. The three images name the same timestamp. A snapshot keeps the old Valid-Until date of its index files, so apt does not check
# that date; it still checks their signatures. apt-get update exits with 0 when a download fails, so --error-on=any stops the build there.
ARG DEBIAN_SNAPSHOT=20261125T230000Z
RUN sed -i -E "s|^URIs: http://deb.debian.org/(debian(-security)?)$|URIs: http://snapshot.debian.org/archive/\1/${DEBIAN_SNAPSHOT}\nCheck-Valid-Until: no|" /etc/apt/sources.list.d/debian.sources \
    && test "$(grep -c "^URIs: http://snapshot.debian.org/archive/debian.*/${DEBIAN_SNAPSHOT}$" /etc/apt/sources.list.d/debian.sources)" = 2 \
    && ! grep -q deb.debian.org /etc/apt/sources.list.d/debian.sources \
    && apt-get update --error-on=any -o Acquire::Retries=5 -o Acquire::http::Timeout=60 \
    && apt-get upgrade --yes --no-install-recommends -o Acquire::Retries=5 -o Acquire::http::Timeout=60 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10003 --user-group --no-create-home --shell /usr/sbin/nologin replay \
    && mkdir -p /opt/replay/app /opt/replay/state /opt/replay/run \
    && chown -R replay:replay /opt/replay

COPY replay/requirements.txt /opt/replay/requirements.txt
# No container runs pip, and the packages that it bundles carry findings: pip goes after the install, with the wheel that would install it again.
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/replay/requirements.txt \
    && python -m pip uninstall --yes pip && rm /usr/local/lib/python3.14/ensurepip/_bundled/pip-*.whl

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
