FROM python:3.13-slim

# Java 25 installieren, das von Spark 4.2 unterstützt wird
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-25-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir pyspark==4.2.0

COPY daily_registration_job.py .

CMD ["python", "daily_registration_job.py"]