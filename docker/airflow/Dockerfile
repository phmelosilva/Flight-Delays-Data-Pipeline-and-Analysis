# Build
FROM python:3.12-slim AS builder

RUN pip install --upgrade pip build

WORKDIR /src

COPY . .

RUN python -m build --wheel --outdir dist

# Final
FROM apache/airflow:3.1.3-python3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        build-essential \
        git \
        curl \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/dist/*.whl /dist/

USER airflow

RUN set -e; for whl in /dist/*.whl; do \
        pip install --no-cache-dir "${whl}[airflow]"; \
    done

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

WORKDIR /opt/airflow
