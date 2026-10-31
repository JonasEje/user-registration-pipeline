FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY producer ./producer
COPY consumer ./consumer
COPY spark ./spark
COPY run.sh ./run.sh
RUN chmod +x ./run.sh && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["./run.sh"]
