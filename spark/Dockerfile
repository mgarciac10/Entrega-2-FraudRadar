# Imagen de Spark 3.5 con Delta Lake, S3 y Kafka, para todos los jobs del proyecto.
# Se construye desde la raíz del repositorio:
#   docker build -f spark/Dockerfile -t fraudradar-spark .
#
# Los jars se descargan al construir (no con --packages al arrancar): así los jobs
# arrancan rápido y no dependen de Internet en tiempo de ejecución.
# Versiones compatibles entre sí: Spark 3.5.3 trae Hadoop 3.3.4, que exige
# aws-java-sdk-bundle 1.12.262; Delta 3.2.x es para Spark 3.5.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ARG DELTA=3.2.1
ARG SPARK=3.5.3
RUN set -eux; cd /opt/spark/jars; \
    get() { curl -fsSL -O "https://repo1.maven.org/maven2/$1"; }; \
    get io/delta/delta-spark_2.12/${DELTA}/delta-spark_2.12-${DELTA}.jar; \
    get io/delta/delta-storage/${DELTA}/delta-storage-${DELTA}.jar; \
    get org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    get com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    get org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK}/spark-sql-kafka-0-10_2.12-${SPARK}.jar; \
    get org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK}/spark-token-provider-kafka-0-10_2.12-${SPARK}.jar; \
    get org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    get org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# API de Python de Delta (DeltaTable.merge) y boto3 (detect_fraud escribe las alertas en DynamoDB y
# silver_to_gold lee su estado). pandas y pyarrow los exige applyInPandasWithState (detect_fraud);
# numpy<2 porque pyarrow 14 y Spark 3.5 no son compatibles con numpy 2.
# PySpark ya viene con la imagen en /opt/spark/python.
RUN python3 -m pip install --no-cache-dir --no-deps delta-spark==${DELTA} \
 && python3 -m pip install --no-cache-dir importlib-metadata boto3 "numpy==1.26.4" "pandas==2.1.4" "pyarrow==14.0.2"

# El código se monta en /opt/fraudradar/spark (compose) o se copia aquí.
ENV PYTHONPATH=/opt/fraudradar/spark/common:/opt/fraudradar/spark/batch:/opt/fraudradar/spark/streaming:${PYTHONPATH}
USER spark
