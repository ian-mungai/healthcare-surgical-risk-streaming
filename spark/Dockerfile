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
# The replaced libraries, each at a release that fixes the findings of Nov 25 2026 and was at least two weeks old, in the major line of the jar that Spark 4.1.4
# holds. Each checksum is of the file whose SHA-1 Maven Central publishes. Netty's modules are one set, 4.2.20.Final with netty-tcnative 2.0.85.Final, which the POM
# and the bill of materials of 4.2.20.Final name. It is a security release (https://netty.io/news/2026/11/02/4-2-20-Final.html) and it holds the fix of
# GHSA-pvjx-v7vp-62vq (netty-codec-http up to 4.2.17.Final, fixed from 4.2.18.Final). Each Netty jar matched the SHA-1 and the SHA-256 that Maven Central publishes.
# lz4-java moved to the group at.yawk.lz4 (org.lz4:lz4-java 1.8.1 is only a relocation to it) and keeps its Java packages. parquet-jackson holds only Parquet's
# relocated copy of Jackson, so it is replaced alone and the other Parquet jars stay at Spark's 1.16.0. Hadoop's two client jars are 3.5.0, its newest release on
# Nov 25 2026, with the same compile dependencies as 3.4.3; Maven Central publishes only their SHA-1, which each file matched.
ADD --checksum=sha256:794136fc6be78e44a286c2b1420a182e135c6d06eb8c397030c1551ee50acccd https://repo1.maven.org/maven2/io/netty/netty-buffer/4.2.20.Final/netty-buffer-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:f6616d7c26c6198c7fa3d94e20a1a0d17e8ef9fc2433de792f4009cf38b00c15 https://repo1.maven.org/maven2/io/netty/netty-codec/4.2.20.Final/netty-codec-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:5a66da26b86d7eecedd9e0d8fe0e22ef1733bbf873e476cb9b860afbed22210c https://repo1.maven.org/maven2/io/netty/netty-codec-base/4.2.20.Final/netty-codec-base-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:7ebae4ac80c01e2d279d0e50d377bcdaf0cf8f2f65797e0578afc810a66ab57d https://repo1.maven.org/maven2/io/netty/netty-codec-compression/4.2.20.Final/netty-codec-compression-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:8296127e74eb871eea6e17520ad5d12aec37700252a61165057b28df6e5f9499 https://repo1.maven.org/maven2/io/netty/netty-codec-dns/4.2.20.Final/netty-codec-dns-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:d08b0d473c48c61ebb2bc4d145111fa1777832aee4d2be1024891e904e36eaf5 https://repo1.maven.org/maven2/io/netty/netty-codec-http/4.2.20.Final/netty-codec-http-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:168790c8d10d2e7cc53fa08906162e3d6c23b4528e7d062eea328d4a281484c6 https://repo1.maven.org/maven2/io/netty/netty-codec-http2/4.2.20.Final/netty-codec-http2-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:49ceb3f4f29f7f29a9acbb31b134b1c886d8fb843278d8dab530bf83cd6dfd0d https://repo1.maven.org/maven2/io/netty/netty-codec-marshalling/4.2.20.Final/netty-codec-marshalling-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:dc995dcfc841c8593c482604d38aab0fa414e9a7c8ea49dc36ebf188064a6470 https://repo1.maven.org/maven2/io/netty/netty-codec-protobuf/4.2.20.Final/netty-codec-protobuf-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:8510f0ceee40a4254760b2927431e2403a6588c4068022e2440356acfc5e24c1 https://repo1.maven.org/maven2/io/netty/netty-codec-socks/4.2.20.Final/netty-codec-socks-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:5807fef7eb619bc0a5d1c0e6c83199ea9bca8503203a3eb73cfdccec2d215743 https://repo1.maven.org/maven2/io/netty/netty-common/4.2.20.Final/netty-common-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:211dbbba074298046b37f841ad9ca89ed91968f6b9e97400c99ab4e6177b1f96 https://repo1.maven.org/maven2/io/netty/netty-handler/4.2.20.Final/netty-handler-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:9fd508051bb39829117e4dbfa2d0b485aafb88d2d627ae6ec1fed5a3faa3842a https://repo1.maven.org/maven2/io/netty/netty-handler-proxy/4.2.20.Final/netty-handler-proxy-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:cb7a6027e4f366fb28dcea7e1f7088ca16bd23c2cc38e8ddfdac77a77bd144dc https://repo1.maven.org/maven2/io/netty/netty-resolver/4.2.20.Final/netty-resolver-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:9a1dc5abc7f146bf0ff0938dceb384f7c972dc052cd9cd2f367cb30a0718fa71 https://repo1.maven.org/maven2/io/netty/netty-resolver-dns/4.2.20.Final/netty-resolver-dns-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:ff4f1f8158c714c1b485ef10cf2456fbd3c1b2930256b70287aeb7dc3ad94783 https://repo1.maven.org/maven2/io/netty/netty-transport/4.2.20.Final/netty-transport-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:814438f990be4eddb997f4bee67617e19bab8554d9ee2c7bd8c9c8adb970bf69 https://repo1.maven.org/maven2/io/netty/netty-transport-classes-epoll/4.2.20.Final/netty-transport-classes-epoll-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:9b47d19a7c39abeeaf3091c38c0e1d7de0a940a0e39fa7ae52defff0f33b8232 https://repo1.maven.org/maven2/io/netty/netty-transport-classes-io_uring/4.2.20.Final/netty-transport-classes-io_uring-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:62be6151dfb8a76a3902e2f0b7ce410482e91015e8ecdc89ee9e0944c30cf6eb https://repo1.maven.org/maven2/io/netty/netty-transport-classes-kqueue/4.2.20.Final/netty-transport-classes-kqueue-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:6f600d5cbd3ce39e50680de66b00d7e6a9652dac7d08a5b022101d717cb7fb52 https://repo1.maven.org/maven2/io/netty/netty-transport-native-unix-common/4.2.20.Final/netty-transport-native-unix-common-4.2.20.Final.jar /opt/spark/jars/
ADD --checksum=sha256:3f13d5fc5b96d1a02209c0082be84b4308736da8abcff18285fbbef3a333143a https://repo1.maven.org/maven2/io/netty/netty-transport-native-epoll/4.2.20.Final/netty-transport-native-epoll-4.2.20.Final-linux-x86_64.jar /opt/spark/jars/
ADD --checksum=sha256:ec22404f0260d75ce7b1b4a3b9b61a1472671cc6b44399c42529cfb42e1f613f https://repo1.maven.org/maven2/io/netty/netty-transport-native-io_uring/4.2.20.Final/netty-transport-native-io_uring-4.2.20.Final-linux-x86_64.jar /opt/spark/jars/
ADD --checksum=sha256:3c1bcba8d80d7002277effe67509979b3a7bd22341bab276b946837f0b76f831 https://repo1.maven.org/maven2/io/netty/netty-tcnative-classes/2.0.85.Final/netty-tcnative-classes-2.0.85.Final.jar /opt/spark/jars/
ADD --checksum=sha256:6ed7189a01c38dd7c2161feb93c5318df3ad67dc1b246d166d7751b7010042d4 https://repo1.maven.org/maven2/io/netty/netty-tcnative-boringssl-static/2.0.85.Final/netty-tcnative-boringssl-static-2.0.85.Final-linux-x86_64.jar /opt/spark/jars/
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
ADD --checksum=sha256:cc59446ec9ebaceae56fdb8d8c21477766dc5d426325642151c796bec5c4e6b7 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.5.0/hadoop-client-api-3.5.0.jar /opt/spark/jars/
ADD --checksum=sha256:b7db3e60c5df1cd37c2995b3e0142e57487260706834e5325bbe0a5e683eedf1 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.5.0/hadoop-client-runtime-3.5.0.jar /opt/spark/jars/
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
