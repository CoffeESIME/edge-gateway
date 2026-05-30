
FROM python:3.11-slim AS base

LABEL maintainer="scada-team"
LABEL description="SCADA Edge Node — Acquisition Microservice"
LABEL version="1.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app_src

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app_src

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
COPY . .

RUN mkdir -p /certs /data \
    && chmod 700 /certs \
    && chmod 755 /data

RUN groupadd -r scada && useradd -r -g scada scada
RUN chown -R scada:scada /app_src /data

USER scada

CMD ["python", "main.py"]
