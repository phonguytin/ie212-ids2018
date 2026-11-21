# One image for the Spark master, workers, the streaming scorer and local batch jobs.
FROM apache/spark:4.0.1
USER root

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Kafka source for Structured Streaming, baked in so nothing is downloaded at job start.
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO $MVN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
 && curl -fsSLO $MVN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
 && curl -fsSLO $MVN/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

ENV PYTHONPATH=/opt/ids/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
WORKDIR /opt/ids
