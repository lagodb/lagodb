ARG SPARK_BASE_IMAGE=apache/spark:4.0.1
FROM ${SPARK_BASE_IMAGE}

ARG ICEBERG_VERSION=1.10.1
ARG ICEBERG_SPARK_RUNTIME=4.0_2.13
ARG MAVEN_REPOSITORY=https://repo.maven.apache.org/maven2
ARG ICEBERG_SPARK_RUNTIME_SHA256=2192a0881ed0f5773b5a83a8820d2b0b2069beec203028643a1c338551007f09
ARG ICEBERG_AWS_BUNDLE_SHA256=86bf20892ea5b4c17688f19b075399885f6aa5303f6b2dc9f491e76ceef9633b

USER root

RUN set -eu; \
    cd "${SPARK_HOME}/jars"; \
    curl -fsSL --retry 3 --retry-delay 5 \
        -o "iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar" \
        "${MAVEN_REPOSITORY}/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar"; \
    curl -fsSL --retry 3 --retry-delay 5 \
        -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "${MAVEN_REPOSITORY}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    printf '%s  %s\n' \
        "${ICEBERG_SPARK_RUNTIME_SHA256}" \
        "iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar" \
        | sha256sum --check -; \
    printf '%s  %s\n' \
        "${ICEBERG_AWS_BUNDLE_SHA256}" \
        "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        | sha256sum --check -; \
    chown spark:spark \
        "iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar" \
        "iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

USER spark
