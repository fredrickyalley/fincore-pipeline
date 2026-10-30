# Local runtime for the whole pipeline: Airflow 2.10 + Java 17 + PySpark 3.5.4 (Glue 5.0
# equivalent) + dbt in its own venv. Production uses MWAA + Glue instead of this image.
# Layers go from heaviest and most stable to lightest and most likely to change, so editing
# the dbt pins or test extras never re-downloads Java, the jars or pyspark.
ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG PYSPARK_VERSION=3.5.4

# large wheels (pyspark is ~300 MB) over slow links: wait longer and retry instead of failing
ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10 \
    JAVA_HOME=/opt/java

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

# s3a connector so local Spark can read/write the S3-compatible store (LocalStack)
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/hadoop-aws.jar \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o /opt/spark-jars/aws-java-sdk-bundle.jar \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

USER airflow
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000 \
    pip install "pyspark==${PYSPARK_VERSION}"

# dbt lives apart from Airflow so their dependency trees cannot conflict.
# Pin dbt-core itself: the adapters alone let pip pick a dbt-core 2.x pre-release.
USER root
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install "dbt-core~=1.9.0" "dbt-postgres~=1.9.0" "dbt-redshift~=1.9.0" \
    && chown -R airflow: /opt/dbt-venv

# the base image already has Airflow; the constraints file keeps these extras compatible with it
USER airflow
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000 \
    pip install pyarrow pytest \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
