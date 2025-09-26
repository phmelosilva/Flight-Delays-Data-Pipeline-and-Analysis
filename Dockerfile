# Build
FROM python:3.12-slim AS builder

RUN pip install --upgrade pip setuptools wheel

WORKDIR /src

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN python setup.py bdist_wheel

# Final

FROM apache/airflow:3.0.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

COPY --from=builder /src/dist/*.whl /dist/

USER airflow

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir /dist/*.whl
