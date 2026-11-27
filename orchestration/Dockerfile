# The image of the three Dagster services: the web server, the daemon and the code location server. Build context: this folder.
# Four more named build contexts hold what the code location needs: `stack_scripts` is the repository's scripts folder, `stack_compose` is its compose folder,
# `stack_spark` is its spark folder and `stack_fixtures` is the folder of the tracked VitalDB pin. Build with: docker build --build-context stack_scripts=<scripts>
# --build-context stack_compose=<compose> --build-context stack_spark=<spark> --build-context stack_fixtures=<tests/fixtures/vitaldb> orchestration
# The Compose plugin, built from the source of the release with fixed Go modules: the plugin that Docker publishes (docker/compose-bin v5.8.0, the newest release on
# Nov 25 2026) links modules with published fixes. The Go image compiles for linux/amd64 on the builder's own platform. The source archive is checked against its
# SHA-256, and each module that go get raises is checked against Go's checksum database. Drop this stage when a Compose release holds these versions.
FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.24@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS compose_build
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOTOOLCHAIN=local
ADD --checksum=sha256:1836019594ad054f8459cea7efc1d52eae7d05c64217c4efffe02d0d2930c44b https://github.com/docker/compose/archive/refs/tags/v5.8.0.tar.gz /tmp/compose.tar.gz
WORKDIR /src
RUN tar -xzf /tmp/compose.tar.gz --strip-components=1 \
    && go get golang.org/x/net@v0.62.0 golang.org/x/crypto@v0.59.0 github.com/klauspost/compress@v1.20.2 \
        go.opentelemetry.io/otel@v1.47.0 go.opentelemetry.io/otel/sdk@v1.47.0 go.opentelemetry.io/otel/trace@v1.47.0 \
    && go build -trimpath -ldflags "-w -X github.com/docker/compose/v5/internal.Version=v5.8.0" -o /out/docker-compose ./cmd

FROM python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/dagster/app \
    DAGSTER_HOME=/opt/dagster/instance \
    STACK_COMPOSE_DIRECTORY=/opt/dagster/stack/compose

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

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/app /opt/dagster/instance/storage /opt/dagster/data /opt/dagster/runs /opt/dagster/staging \
    && chown -R dagster:dagster /opt/dagster

COPY requirements.txt /opt/dagster/requirements.txt
# No container runs pip, and the packages that it bundles carry findings: pip goes after the install, with the wheel that would install it again.
RUN pip install --require-hashes --only-binary :all: --no-deps --requirement /opt/dagster/requirements.txt \
    && python -m pip uninstall --yes pip && rm /usr/local/lib/python3.14/ensurepip/_bundled/pip-*.whl

# The Docker command line, pinned by version and by the digest of the official image that holds it, and the Compose plugin that the first stage built.
COPY --from=docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d /usr/local/bin/docker /usr/local/bin/docker
COPY --from=compose_build /out/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

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
    ingestion/raw_copy.py ingestion/record_run.py ingestion/record_state.py ingestion/selection.py ingestion/store.py ingestion/table_checks.py ingestion/track_list.py \
    ingestion/connection.py \
    /opt/dagster/app/scripts/ingestion/
COPY --from=stack_scripts sample/__init__.py sample/candidates.py sample/publisher.py sample/split.py /opt/dagster/app/scripts/sample/
COPY --from=stack_spark __init__.py table_checks.py /opt/dagster/app/spark/
COPY --from=stack_fixtures release.json /opt/dagster/app/tests/fixtures/vitaldb/release.json
# The candidate list that the raw copy's fixed-sample selection reads when it checks the connection's globs.
COPY --from=stack_fixtures candidates.tsv /opt/dagster/app/tests/fixtures/vitaldb/candidates.tsv
COPY --from=stack_compose compose.yml limits.yml /opt/dagster/stack/compose/
COPY --from=stack_compose services /opt/dagster/stack/compose/services

COPY dagster.yaml workspace.yaml /opt/dagster/instance/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY surgical_risk /opt/dagster/app/surgical_risk

USER 10001:10001
WORKDIR /opt/dagster/instance
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
