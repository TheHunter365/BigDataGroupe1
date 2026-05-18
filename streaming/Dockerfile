FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		curl \
		ca-certificates \
		openjdk-21-jre-headless \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN pip install --no-cache-dir apache-flink kafka-python

RUN mkdir -p /opt/flink/lib \
	&& curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
	  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

COPY flink.py kafka-producer.py ./

ENTRYPOINT ["python"]
CMD ["kafka-producer.py"]
