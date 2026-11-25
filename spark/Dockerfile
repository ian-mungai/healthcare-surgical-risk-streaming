# The image of every Spark job: Python 3.14, Java 21, Spark 4.1.4 on Scala 2.13, Iceberg's Spark runtime (it holds the native Nessie catalog) and
# Iceberg's AWS bundle (the S3 file system), Spark's Kafka source and sink with the Kafka client, and the Python packages of the Bronze decoder and the streaming job
# (spark/requirements.txt, with hashes, wheels only). Every download is checked against its SHA-256 at build, and a job fetches nothing when it starts.
# Build and push: make images-push (scripts/images/publish.py stages the folder from HEAD, builds this file for linux/amd64 and pins the pushed digest).
# SPARK_MIRROR names another host for the same Spark file. The checksum still decides whether the build accepts it.
ARG PYTHON_IMAGE=python:3.14.10-slim-trixie@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b
ARG JRE_IMAGE=eclipse-temurin:21.0.13_9-jre-noble@sha256:b4f9d2a0bc273417cf5219b7b832bbf0493ed500b7b57d5001d9f83374659060
ARG SPARK_VERSION=4.1.4
ARG SCALA_VERSION=2.13
ARG ICEBERG_VERSION=1.12.0
ARG SPARK_MIRROR=https://archive.apache.org/dist/spark
ARG SPARK_SHA256=e9430aa52bd947cc0d3f1299afe5089dcd262166cd202ba6aa77095b8541642b
ARG ICEBERG_RUNTIME_SHA256=3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82
ARG ICEBERG_AWS_BUNDLE_SHA256=dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957
# The Kafka connector of Spark 4.1.4 and its compile dependencies that the distribution does not hold, as its pom lists them (Maven Central, read Nov 23 2026).
# The distribution already holds scala-parallel-collections 1.2.0 (dev/deps/spark-deps-hadoop-3-hive-2.3 of the v4.1.4 tag), so it is not added again.
ARG KAFKA_CLIENTS_VERSION=3.9.2
ARG COMMONS_POOL_VERSION=2.12.1
ARG SPARK_KAFKA_SHA256=94f9514f64b319aa31c3b2061bf2fb75700f82e14845fda71337a13696855710
ARG SPARK_KAFKA_PROVIDER_SHA256=b641257bf720279865206a9cd8d69691c8004bc18e653829c6e1aa1d748c6716
ARG KAFKA_CLIENTS_SHA256=014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8
ARG COMMONS_POOL_SHA256=5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef

FROM --platform=linux/amd64 ${JRE_IMAGE} AS jre

FROM --platform=linux/amd64 ${PYTHON_IMAGE} AS distribution
ARG SPARK_VERSION
ARG SPARK_MIRROR
ARG SPARK_SHA256
ADD --checksum=sha256:${SPARK_SHA256} ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir /opt/spark && tar --extract --gzip --file /tmp/spark.tgz --directory /opt/spark --strip-components=1 && rm -rf /opt/spark/examples /opt/spark/R /opt/spark/data
# The jars that no job needs go before the copy, so no layer of the final image holds them (checked on Nov 25 2026: no project file enables Hive support or
# names a Hive metastore, Kubernetes, YARN or Spark Connect): Hive with its Derby database, Thrift and DataNucleus (the session keeps Spark's in-memory catalog,
# and the tables are Iceberg tables in Nessie; hive-storage-api stays, because Spark's ORC code uses it), Spark's Kubernetes support with its client and Vert.x,
# Spark's YARN support and shuffle service, the Connect REPL, the Scala shell's JLine 3 and Netty's QUIC and HTTP/3. A name that matches no file stops the build.
RUN rm -r /opt/spark/jars/connect-repl /opt/spark/yarn /opt/spark/kubernetes \
    && rm /opt/spark/jars/hive-beeline-*.jar /opt/spark/jars/hive-cli-*.jar /opt/spark/jars/hive-common-*.jar /opt/spark/jars/hive-exec-*.jar \
        /opt/spark/jars/hive-jdbc-*.jar /opt/spark/jars/hive-metastore-*.jar /opt/spark/jars/hive-serde-*.jar /opt/spark/jars/hive-service-rpc-*.jar \
        /opt/spark/jars/hive-shims-*.jar /opt/spark/jars/spark-hive_*.jar /opt/spark/jars/spark-hive-thriftserver_*.jar /opt/spark/jars/derby*.jar \
        /opt/spark/jars/datanucleus-*.jar /opt/spark/jars/javax.jdo-*.jar /opt/spark/jars/jdo-api-*.jar /opt/spark/jars/libthrift-*.jar /opt/spark/jars/libfb303-*.jar \
    && rm /opt/spark/jars/kubernetes-*.jar /opt/spark/jars/spark-kubernetes_*.jar /opt/spark/jars/vertx-*.jar /opt/spark/jars/zjsonpatch-*.jar \
        /opt/spark/jars/spark-yarn_*.jar /opt/spark/jars/jline-3.*.jar \
    && rm /opt/spark/jars/netty-codec-classes-quic-*.jar /opt/spark/jars/netty-codec-native-quic-*.jar /opt/spark/jars/netty-codec-http3-*.jar
# The libraries that the final stage adds again at a fixed version. Netty's native libraries come back for linux x86-64 only.
RUN rm /opt/spark/jars/netty-*.jar /opt/spark/jars/jackson-core-*.jar /opt/spark/jars/jackson-databind-*.jar /opt/spark/jars/jackson-dataformat-yaml-*.jar \
    /opt/spark/jars/jackson-datatype-jsr310-*.jar /opt/spark/jars/jackson-module-scala_*.jar /opt/spark/jars/log4j-*.jar /opt/spark/jars/guava-*.jar \
    /opt/spark/jars/zookeeper-*.jar /opt/spark/jars/snappy-java-*.jar /opt/spark/jars/lz4-java-*.jar /opt/spark/jars/parquet-jackson-*.jar \
    /opt/spark/jars/hadoop-client-api-*.jar /opt/spark/jars/hadoop-client-runtime-*.jar

FROM --platform=linux/amd64 ${PYTHON_IMAGE}
ARG SPARK_VERSION
ARG SCALA_VERSION
ARG ICEBERG_VERSION
ARG ICEBERG_RUNTIME_SHA256
ARG ICEBERG_AWS_BUNDLE_SHA256
ARG KAFKA_CLIENTS_VERSION
ARG COMMONS_POOL_VERSION
ARG SPARK_KAFKA_SHA256
ARG SPARK_KAFKA_PROVIDER_SHA256
ARG KAFKA_CLIENTS_SHA256
ARG COMMONS_POOL_SHA256
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=distribution /opt/spark /opt/spark
# Debian's security and point-release updates that the base image does not hold yet.
RUN apt-get update && apt-get upgrade --yes --no-install-recommends && rm -rf /var/lib/apt/lists/*
# The replaced libraries, each at a release that fixes the findings of Nov 25 2026 and was at least two weeks old, in the major line of the jar that Spark 4.1.4
# holds. Each checksum is of the file whose SHA-1 Maven Central publishes. Netty 4.2.17.Final names netty-tcnative 2.0.81.Final in its bill of materials.
# lz4-java moved to the group at.yawk.lz4 (org.lz4:lz4-java 1.8.1 is only a relocation to it) and keeps its Java packages. parquet-jackson holds only Parquet's
# relocated copy of Jackson, so it is replaced alone and the other Parquet jars stay at Spark's 1.16.0.
ADD --checksum=sha256:0d249178ec0204b35a6ad3e5ac8a6c2ecb3eb496c6cdae40b8eca4fa3cb6a0c7 https://repo1.maven.org/maven2/io/netty/netty-buffer/4.2.17.Final/netty-buffer-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:ac76f972446a2f72818441f746d0e73c58519a4687bf680d2b687d7dc5e28e2e https://repo1.maven.org/maven2/io/netty/netty-codec/4.2.17.Final/netty-codec-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:3b92bc3b7d231fa6f4889b2b9281cae6b634428b8121fb1b9b5fccba1543d352 https://repo1.maven.org/maven2/io/netty/netty-codec-base/4.2.17.Final/netty-codec-base-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:425b287a1f36986a5c8edd76379a49decdf99d284f3454f3fa607a073bfb2c18 https://repo1.maven.org/maven2/io/netty/netty-codec-compression/4.2.17.Final/netty-codec-compression-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:394f4649a824683735f28b299d0a48caff059ae30e654e2abf8b3f392af4d374 https://repo1.maven.org/maven2/io/netty/netty-codec-dns/4.2.17.Final/netty-codec-dns-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:a5bde91ac82ab90579df6559c0bd7ea6ae37460e3f18116ab4122194283b3669 https://repo1.maven.org/maven2/io/netty/netty-codec-http/4.2.17.Final/netty-codec-http-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:450af97424cf58d2d72ad4d476d9e290f2fdf69fa181c289a6e6dd176bb86a7b https://repo1.maven.org/maven2/io/netty/netty-codec-http2/4.2.17.Final/netty-codec-http2-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:ee5ffed638da6b7e2b4567559bd5f78b18f635cdaa4f3b6d23aa27ccf6cb57c2 https://repo1.maven.org/maven2/io/netty/netty-codec-marshalling/4.2.17.Final/netty-codec-marshalling-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:44b2fcc4bc1d6c01edafe6ae6c8a7de7c3c5bad59697a93b6b879a45a78a5387 https://repo1.maven.org/maven2/io/netty/netty-codec-protobuf/4.2.17.Final/netty-codec-protobuf-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:6b5760cc512cee1f7515e25010892471f3995045f84cd6c1d1b6e6cee534733c https://repo1.maven.org/maven2/io/netty/netty-codec-socks/4.2.17.Final/netty-codec-socks-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:502aae2a6680e9bca3558a3fa098f06dc4d87340b8b714e925a6d690871c092a https://repo1.maven.org/maven2/io/netty/netty-common/4.2.17.Final/netty-common-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:4df11c7520b556c5e2c84b939182699d48fcbb09e97a6bb5e1c0b6835227d126 https://repo1.maven.org/maven2/io/netty/netty-handler/4.2.17.Final/netty-handler-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:94089bea6ba6f3248725f2bf233aa2bc70ac1547c21b9bdfc7548f710a845aa0 https://repo1.maven.org/maven2/io/netty/netty-handler-proxy/4.2.17.Final/netty-handler-proxy-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:7a4b599b9c0c29a505d3b7311f142df883fb27d0f67495a7cbaf533b0e456623 https://repo1.maven.org/maven2/io/netty/netty-resolver/4.2.17.Final/netty-resolver-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:d332481c22136a68c2ee9e45e86212174932587d93a5dd4d201e196828bf9f91 https://repo1.maven.org/maven2/io/netty/netty-resolver-dns/4.2.17.Final/netty-resolver-dns-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:d2409ce3a735cbab4188d0ea5d82abed823a6a07a8389f49af4a9e1243a562bc https://repo1.maven.org/maven2/io/netty/netty-transport/4.2.17.Final/netty-transport-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:4e21d8a2e3429d9de967a716d58119f57585786bd2f7213bf5b27770425269de https://repo1.maven.org/maven2/io/netty/netty-transport-classes-epoll/4.2.17.Final/netty-transport-classes-epoll-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:c6dafc0fd8514b816b9ba724a031c21de6b0fe3215773feb38b6b0bff15a9f34 https://repo1.maven.org/maven2/io/netty/netty-transport-classes-io_uring/4.2.17.Final/netty-transport-classes-io_uring-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:f0b2dd4757a22345b0b28e2856dc7ee44e5dea256cc520ddc27e4dce4ea9efb3 https://repo1.maven.org/maven2/io/netty/netty-transport-classes-kqueue/4.2.17.Final/netty-transport-classes-kqueue-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:e01aa467b9104de54a57c08c290f70b22e53ec97cf5583c87c112170ef5832cc https://repo1.maven.org/maven2/io/netty/netty-transport-native-unix-common/4.2.17.Final/netty-transport-native-unix-common-4.2.17.Final.jar /opt/spark/jars/
ADD --checksum=sha256:c295b1d7f46e3c7375c93fa4c8abcc3944398c5e4eb53e31587cbbf0bf559d4f https://repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/4.2.17.Final/netty-transport-native-epoll-4.2.17.Final-linux-x86_64.jar /opt/spark/jars/
ADD --checksum=sha256:94ea8dc7e00949949a1432f18cbf020388f35c8e1e2b6be77490ca557672f2f3 https://repo1.maven.org/maven2/io/netty/netty-transport-native-io_uring/4.2.17.Final/netty-transport-native-io_uring-4.2.17.Final-linux-x86_64.jar /opt/spark/jars/
ADD --checksum=sha256:b317bff5c41fea5a2c2eefebcc25c5ee486726c93e580200bc4c9ec7fc33e1f2 https://repo1.maven.org/maven2/io/netty/netty-tcnative-classes/2.0.81.Final/netty-tcnative-classes-2.0.81.Final.jar /opt/spark/jars/
ADD --checksum=sha256:9c7ce1e5965a9cd2a9bc57c602f3917e3994279e9c701953e8f6c6aefb76f431 https://repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.81.Final/netty-tcnative-boringssl-static-2.0.81.Final-linux-x86_64.jar /opt/spark/jars/
ADD --checksum=sha256:8c5623b98f32d5f7e1287ff61ed50e08ab7b11381e991fff89327faabba42261 https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.21.7/jackson-core-2.21.7.jar /opt/spark/jars/
ADD --checksum=sha256:1290c2795e93e8a6861a6c4d9ff0d844d32f5ea178362cb2721edf5561e828b1 https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.21.7/jackson-databind-2.21.7.jar /opt/spark/jars/
ADD --checksum=sha256:df2b3e6a194181dc067b7211d750926b4ae9e4f3c892a0005b8219a9b59be681 https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/2.21.7/jackson-dataformat-yaml-2.21.7.jar /opt/spark/jars/
ADD --checksum=sha256:5989b937f0aa5b6407e7a061077c9bfcac0590dc0a32e54b2fac6a3e2d49e812 https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jsr310/2.21.7/jackson-datatype-jsr310-2.21.7.jar /opt/spark/jars/
ADD --checksum=sha256:f2963e4f184b581957f15086997800a6cd8eb865234d28274c3b814c911b9629 https://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-scala_2.13/2.21.7/jackson-module-scala_2.13-2.21.7.jar /opt/spark/jars/
ADD --checksum=sha256:64777f73ea0b3104c04eb82befbdccc30a425a19e83ad06cb2f93aa303511863 https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.25.5/log4j-api-2.25.5.jar /opt/spark/jars/
ADD --checksum=sha256:050c4f85deb48b055e9c041ac39043fbb79ce477841a11a1eb2969c86c6a0072 https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.25.5/log4j-core-2.25.5.jar /opt/spark/jars/
ADD --checksum=sha256:4dd812dc5a6343f542a9e0046b1ec78ecf10bdd5a8c15745101cdd8b9aa24974 https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-1.2-api/2.25.5/log4j-1.2-api-2.25.5.jar /opt/spark/jars/
ADD --checksum=sha256:8cab16dd6fc4c8f14c1c473022cb217512f2bbb2d59a70f8ec90a268d64d8831 https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j2-impl/2.25.5/log4j-slf4j2-impl-2.25.5.jar /opt/spark/jars/
ADD --checksum=sha256:27c37313e20c16bf0178e38d9d1a5c7320e34c03aa35bc2c66a589ea14abf545 https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/2.25.5/log4j-layout-template-json-2.25.5.jar /opt/spark/jars/
ADD --checksum=sha256:b530942257fb935f8b2cfaa5f8eb5bd59c555fd8e8d01b8ce98912e077ea606c https://repo1.maven.org/maven2/com/google/guava/guava/33.7.2-jre/guava-33.7.2-jre.jar /opt/spark/jars/
ADD --checksum=sha256:83c722c769bf63bd3890c547f1bccb054bb8669e2c6feb6e407a73c3d9fcba55 https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.9.7/zookeeper-3.9.7.jar /opt/spark/jars/
ADD --checksum=sha256:1f659b0a84518c28b3cc94a143d6139e5e49ec711ae5d942442e7b1e7fd47177 https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper-jute/3.9.7/zookeeper-jute-3.9.7.jar /opt/spark/jars/
ADD --checksum=sha256:35228628442e4b9797681c7cbd3b26594f6da3379159dbd0cb07094ed3aece0d https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.9/snappy-java-1.1.10.9.jar /opt/spark/jars/
ADD --checksum=sha256:58c8e0b813960d2a248e050c353baea73139b48a2ffc55382d42c69707e17325 https://repo1.maven.org/maven2/at/yawk/lz4/lz4-java/1.11.4/lz4-java-1.11.4.jar /opt/spark/jars/
ADD --checksum=sha256:dc20428b547c6376ae7dfa20c30a62c326b7e4ed4915ba5370ae7d7b0859408d https://repo1.maven.org/maven2/org/apache/parquet/parquet-jackson/1.18.2/parquet-jackson-1.18.2.jar /opt/spark/jars/
ADD --checksum=sha256:6443332781f26a7199009d9517cd1eb013fbf54ca1c9759a2a9e487542b1d52a https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar /opt/spark/jars/
ADD --checksum=sha256:bd20267b0555766ae84697dd888f074ca326d8e2ec3c0008928a6ac2966c67de https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar /opt/spark/jars/
ADD --checksum=sha256:${ICEBERG_RUNTIME_SHA256} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${ICEBERG_AWS_BUNDLE_SHA256} https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${SPARK_KAFKA_SHA256} https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${SPARK_KAFKA_PROVIDER_SHA256} https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${KAFKA_CLIENTS_SHA256} https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --checksum=sha256:${COMMONS_POOL_SHA256} https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar /opt/spark/jars/
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark PYSPARK_PYTHON=python3 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENV PATH=/opt/java/openjdk/bin:/opt/spark/bin:${PATH} PYTHONPATH=/opt/jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
RUN groupadd --gid 10100 spark && useradd --uid 10100 --gid 10100 --no-create-home --home-dir /tmp --shell /usr/sbin/nologin spark \
    && mkdir /scratch /warehouse /checkpoints && chown 10100:10100 /scratch /warehouse /checkpoints && chmod 0755 /opt/spark/jars/*.jar
COPY spark/requirements.txt /tmp/requirements.txt
# No container runs pip, and the packages that it bundles carry findings: pip goes after the install, with the wheel that would install it again.
RUN pip install --no-cache-dir --require-hashes --only-binary :all: --no-deps --requirement /tmp/requirements.txt && rm /tmp/requirements.txt \
    && python -m pip uninstall --yes pip && rm /usr/local/lib/python3.14/ensurepip/_bundled/pip-*.whl
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
COPY features/warning_label.py /opt/jobs/features/warning_label.py
# The signal profile job counts the unscored cutoffs by the sample limits of the feature registry (standard library only).
COPY features/registry.py /opt/jobs/features/registry.py
COPY features/model.py /opt/jobs/features/model.py
COPY features/stats.py /opt/jobs/features/stats.py
# The streaming job builds each feature row with the feature code, as the batch job does: the row builder, its groups, the eligibility limits and the leakage check (standard library only).
COPY features/batch.py /opt/jobs/features/batch.py
COPY features/clinical.py /opt/jobs/features/clinical.py
COPY features/document.py /opt/jobs/features/document.py
COPY features/eligibility.py /opt/jobs/features/eligibility.py
COPY features/leakage.py /opt/jobs/features/leakage.py
COPY features/numeric.py /opt/jobs/features/numeric.py
COPY features/row.py /opt/jobs/features/row.py
COPY features/sampling.py /opt/jobs/features/sampling.py
COPY features/series_input.py /opt/jobs/features/series_input.py
COPY features/validate.py /opt/jobs/features/validate.py
COPY features/waveform.py /opt/jobs/features/waveform.py
# The Bronze decoder imports the shared case-file package (the decoding helpers and the read-only stores that the replay uses too), and the streaming job imports the replay's event model and
# wire codec. Both import the ingestion gate, and the gate reads the tracked release pin through ROOT.
COPY casefile /opt/jobs/casefile
COPY replay/__init__.py /opt/jobs/replay/__init__.py
COPY replay/events/__init__.py /opt/jobs/replay/events/__init__.py
COPY replay/events/errors.py /opt/jobs/replay/events/errors.py
COPY replay/events/event_list.py /opt/jobs/replay/events/event_list.py
COPY replay/events/formats.py /opt/jobs/replay/events/formats.py
COPY replay/events/schemas /opt/jobs/replay/events/schemas
COPY replay/events/wire.py /opt/jobs/replay/events/wire.py
COPY replay/events/identity.py /opt/jobs/replay/events/identity.py
COPY replay/events/recording.py /opt/jobs/replay/events/recording.py
COPY replay/events/schedule.py /opt/jobs/replay/events/schedule.py
COPY replay/events/stream.py /opt/jobs/replay/events/stream.py
COPY replay/events/waveform.py /opt/jobs/replay/events/waveform.py
COPY replay/service/__init__.py /opt/jobs/replay/service/__init__.py
COPY replay/service/registry.py /opt/jobs/replay/service/registry.py
COPY replay/service/wire_codec.py /opt/jobs/replay/service/wire_codec.py
COPY scripts/process.py /opt/jobs/scripts/process.py
COPY scripts/project_config.py /opt/jobs/scripts/project_config.py
COPY scripts/ingestion /opt/jobs/scripts/ingestion
COPY scripts/sample /opt/jobs/scripts/sample
COPY tests/fixtures/vitaldb/release.json /opt/jobs/tests/fixtures/vitaldb/release.json
WORKDIR /opt/jobs
USER 10100:10100
ENTRYPOINT ["python", "-m"]
