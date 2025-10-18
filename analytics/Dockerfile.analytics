FROM python:3.12-slim

WORKDIR /home/jovyan

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install jupyterlab notebook ipykernel

COPY ../../pyproject.toml ./

RUN pip install ".[analytics]"

EXPOSE 8888
