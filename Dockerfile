# Build
FROM python:3.12-slim AS builder

RUN pip install --upgrade pip build

WORKDIR /src

COPY . .

RUN python -m build --wheel --outdir dist

# Final
FROM apache/airflow:3.0.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/dist/*.whl /dist/

USER airflow

RUN for whl in /dist/*.whl; do \
        pip install --no-cache-dir "${whl}[dev]"; \
    done
