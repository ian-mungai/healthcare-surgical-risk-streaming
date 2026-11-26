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
# rebuild installs the same packages. The three images run this same step. It stops unless the base image's sources are exactly its main, updates and security
# suites, then writes the one source file that both apt commands read, and no other source. The upgrade starts only when apt-get indextargets exits with 0 and the
# indexes that it names are those three at the snapshot. The base image holds the CA certificates, so apt reads the snapshot over HTTPS and verifies the server:
# a signed index alone could be an older one, because a snapshot keeps the old Valid-Until date of its index files and apt does not check that date (it still
# checks their signatures). apt-get update exits with 0 when a download fails, so --error-on=any stops the build there.
ARG DEBIAN_SNAPSHOT=20261125T230000Z
RUN list=/etc/apt/sources.list.d/debian.sources archive=https://snapshot.debian.org/archive \
    && stanza() { printf 'Types: deb\nURIs: %s\nSuites: %s\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.pgp\n' "$1" "$2"; } \
    && test ! -e /etc/apt/sources.list && test ! -L /etc/apt/sources.list \
    && test "$(ls -A /etc/apt/sources.list.d/)" = debian.sources && test -f "$list" && test ! -L "$list" \
    && test "$(grep -v '^#' "$list")" = "$(stanza http://deb.debian.org/debian 'trixie trixie-updates' && echo && stanza http://deb.debian.org/debian-security trixie-security)" \
    && { stanza "$archive/debian/$DEBIAN_SNAPSHOT" 'trixie trixie-updates' && printf 'Check-Valid-Until: no\n\n' \
        && stanza "$archive/debian-security/$DEBIAN_SNAPSHOT" trixie-security && echo 'Check-Valid-Until: no'; } > "$list" \
    && set -- -o "Dir::Etc::sourcelist=$list" -o Dir::Etc::sourceparts=- -o Acquire::Retries=5 -o Acquire::https::Timeout=60 \
    && apt-get update --error-on=any "$@" \
    && held="$(apt-get indextargets "$@" --format '$(REPO_URI) $(RELEASE) $(COMPONENT)' 'Created-By: Packages')" \
    && test "$(printf '%s\n' "$held" | LC_ALL=C sort -u)" \
        = "$(printf "$archive/%s/$DEBIAN_SNAPSHOT/ %s main\n" debian-security trixie-security debian trixie debian trixie-updates)" \
    && apt-get upgrade --yes --no-install-recommends "$@" \
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
