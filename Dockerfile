# Build Stage
FROM python:3.11-slim as builder

# Define um prefixo de instalação para manter tudo organizado
ENV INSTALL_PATH=/install

# Cria o diretório e define as permissões
RUN mkdir -p ${INSTALL_PATH} && chown -R 1000:0 ${INSTALL_PATH}

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=${INSTALL_PATH} -r requirements.txt

# Installation Stage

FROM apache/airflow:3.0.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Localiza o diretório 'site-packages' do Python na imagem do Airflow
ENV PYTHON_SITE_PACKAGES=/usr/local/lib/python3.11/site-packages

# Copia apenas as bibliotecas instaladas do 'builder' para a imagem final
COPY --from=builder --chown=airflow:0 /install/lib/python3.11/site-packages/* ${PYTHON_SITE_PACKAGES}/

# Copia os executáveis (como jupyter, pyspark) para um diretório no PATH
COPY --from=builder --chown=airflow:0 /install/bin/* /usr/local/bin/

USER airflow
