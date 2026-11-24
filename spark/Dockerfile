# The image of every Spark job: Python 3.14, Java 21, Spark 4.1.4 on Scala 2.13, Iceberg's Spark runtime (it holds the native Nessie catalog) and
# Iceberg's AWS bundle (the S3 file system), and the Python packages of the Bronze decoder (spark/requirements.txt, with hashes, wheels only). Every download is
# checked against its SHA-256 at build, and a job fetches nothing when it starts.
# Build: docker build --platform linux/amd64 --file spark/Dockerfile --tag surgical-risk-spark:4.1.4 .
# SPARK_MIRROR names another host for the same Spark file. The checksum still decides whether the build accepts it.
ARG PYTHON_IMAGE=python:3.14.10-slim-trixie@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f
ARG JRE_IMAGE=eclipse-temurin:21.0.13_9-jre-noble@sha256:b4f9d2a0bc273417cf5219b7b832bbf0493ed500b7b57d5001d9f83374659060
ARG SPARK_VERSION=4.1.4
ARG SCALA_VERSION=2.13
ARG ICEBERG_VERSION=1.12.0
ARG SPARK_MIRROR=https://archive.apache.org/dist/spark
ARG SPARK_SHA256=e9430aa52bd947cc0d3f1299afe5089dcd262166cd202ba6aa77095b8541642b
ARG ICEBERG_RUNTIME_SHA256=3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82
ARG ICEBERG_AWS_BUNDLE_SHA256=dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957

FROM --platform=linux/amd64 ${JRE_IMAGE} AS jre

FROM --platform=linux/amd64 ${PYTHON_IMAGE} AS distribution
ARG SPARK_VERSION
ARG SPARK_MIRROR
ARG SPARK_SHA256
ADD --checksum=sha256:${SPARK_SHA256} ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir /opt/spark && tar --extract --gzip --file /tmp/spark.tgz --directory /opt/spark --strip-components=1 && rm -rf /opt/spark/examples /opt/spark/R /opt/spark/data

FROM --platform=linux/amd64 ${PYTHON_IMAGE}
ARG SPARK_VERSION
ARG SCALA_VERSION
ARG ICEBERG_VERSION
ARG ICEBERG_RUNTIME_SHA256
ARG ICEBERG_AWS_BUNDLE_SHA256
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=distribution /opt/spark /opt/spark
ADD --checksum=sha256:${ICEBERG_RUNTIME_SHA256} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${ICEBERG_AWS_BUNDLE_SHA256} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark PYSPARK_PYTHON=python3 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENV PATH=/opt/java/openjdk/bin:/opt/spark/bin:${PATH} PYTHONPATH=/opt/jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
RUN groupadd --gid 10100 spark && useradd --uid 10100 --gid 10100 --no-create-home --home-dir /tmp --shell /usr/sbin/nologin spark \
    && mkdir /scratch /warehouse && chown 10100:10100 /scratch /warehouse && chmod 0755 /opt/spark/jars/*.jar
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes --only-binary :all: --no-deps --requirement /tmp/requirements.txt && rm /tmp/requirements.txt
COPY spark /opt/jobs/spark
COPY scripts/__init__.py /opt/jobs/scripts/__init__.py
COPY scripts/stack/__init__.py /opt/jobs/scripts/stack/__init__.py
COPY scripts/stack/nessie.py /opt/jobs/scripts/stack/nessie.py
COPY scripts/stack/tags.py /opt/jobs/scripts/stack/tags.py
# The Silver jobs call the one definition of validity, of the pressure series, of the events and of the labels that the feature code shares (standard library only).
COPY features/__init__.py /opt/jobs/features/__init__.py
COPY features/signal_validity.py /opt/jobs/features/signal_validity.py
COPY features/pressure_series.py /opt/jobs/features/pressure_series.py
COPY features/hypotension.py /opt/jobs/features/hypotension.py
COPY features/label_values.py /opt/jobs/features/label_values.py
COPY features/icu_label.py /opt/jobs/features/icu_label.py
COPY features/kidney_label.py /opt/jobs/features/kidney_label.py
# The Bronze decoder imports the replay's decoder and stores and the ingestion gate, and the gate reads the tracked release pin through ROOT.
COPY replay/__init__.py /opt/jobs/replay/__init__.py
COPY replay/events/__init__.py /opt/jobs/replay/events/__init__.py
COPY replay/events/errors.py /opt/jobs/replay/events/errors.py
COPY replay/events/identity.py /opt/jobs/replay/events/identity.py
COPY replay/events/recording.py /opt/jobs/replay/events/recording.py
COPY replay/events/schedule.py /opt/jobs/replay/events/schedule.py
COPY replay/events/stream.py /opt/jobs/replay/events/stream.py
COPY replay/events/waveform.py /opt/jobs/replay/events/waveform.py
COPY replay/service/__init__.py /opt/jobs/replay/service/__init__.py
COPY replay/service/decode.py /opt/jobs/replay/service/decode.py
COPY replay/service/folder_store.py /opt/jobs/replay/service/folder_store.py
COPY replay/service/s3_store.py /opt/jobs/replay/service/s3_store.py
COPY scripts/process.py /opt/jobs/scripts/process.py
COPY scripts/project_config.py /opt/jobs/scripts/project_config.py
COPY scripts/ingestion /opt/jobs/scripts/ingestion
COPY scripts/sample /opt/jobs/scripts/sample
COPY tests/fixtures/vitaldb/release.json /opt/jobs/tests/fixtures/vitaldb/release.json
WORKDIR /opt/jobs
USER 10100:10100
ENTRYPOINT ["python", "-m"]
