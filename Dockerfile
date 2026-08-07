FROM python:3.11-slim

# Spark can PySpark can chay duoc thi can Java (JVM). Day la ly do nhieu nguoi
# moi hoc Spark bi loi "JAVA_HOME is not set" - vi PySpark ban chat la mot lop
# Python wrapper goi vao Spark engine viet bang Scala/Java, chay tren JVM.
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]
