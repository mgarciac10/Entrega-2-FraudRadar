FROM apache/airflow:2.10.3-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "pyspark==3.5.3" "delta-spark==3.2.1" boto3

USER root
RUN set -eux; \
    mkdir -p /opt/spark-extra-jars; \
    curl -fsSL -o /opt/spark-extra-jars/delta-spark_2.12-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar; \
    curl -fsSL -o /opt/spark-extra-jars/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar; \
    curl -fsSL -o /opt/spark-extra-jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    curl -fsSL -o /opt/spark-extra-jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    chown -R airflow:root /opt/spark-extra-jars

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark \
    PATH=/home/airflow/.local/bin:${PATH}
