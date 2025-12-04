# Flight Delays Data Pipeline and Analysis

Este projeto implementa um pipeline completo de engenharia de dados desenvolvido para a disciplina de **Sistemas de Bancos de Dados 2**, demonstrando a construção de uma solução de dados de ponta a ponta, desde a ingestão de arquivos brutos até a criação de um modelo analítico em formato dimensional. Utilizando os [dados de atrasos de voos dos EUA (2015)](https://www.kaggle.com/datasets/usdot/flight-delays), provenientes do Kaggle, o projeto adota a **Medallion Architecture (Bronze, Silver, Gold)** para estruturar, transformar e validar dados de forma incremental e reproduzível.

A solução simula um ambiente realista de produção, incluindo orquestração com Apache Airflow, processamento distribuído com PySpark, execução parametrizada via Papermill, transformações com o dbt, modelagem analítica e um Data Warehouse em PostgreSQL.

---

## Tecnologias utilizadas

- **Orquestração:** Apache Airflow;
- **Conteinerização:** Docker & Docker Compose;
- **Armazenamento:** Csv e Parquet;
- **Banco de Dados (Metadados do Airflow e Data Warehouse):** PostgreSQL;
- **Análise e Processamento de Dados:** dbt, Jupyter Notebook, Matplotlib, Pandas, Papermill, PySpark, Seaborn e Scikit-learn;
- **Visualização:** Microsoft PowerBI;
- **Linguagens:** Python, SQL & Bash.

---

## Arquitetura Geral

A pipeline segue a arquitetura Medallion:

- **Stage:** Área inicial contendo os arquivos csv brutos, utilizados diretamente como input da camada Bronze (evita redownloads dos arquivos pesados);
- **Bronze:** Conversão para Parquet e persistência final dos arquivos brutos;
- **Silver:** Limpeza, normalização, tratamento de tipos, padronização de colunas e validações de qualidade (quality gates);
- **Gold:** Modelagem analítica em esquema estrela, com criação de dimensões e fatos, otimizada para uso em dashboards e consultas de alto desempenho.

---

## Estrutura do projeto

```
.
├── .dockerignore
├── .env.example
├── .gitattributes
├── .github
├── .gitignore
├── README.md
├── airflow
│   └── dags
│       └── pl_raw_to_gold.py
│
├── data-layer
│   ├── gold
│   │   ├── gold_consultas.sql
│   │   ├── gold_data_dictionary.md
│   │   ├── gold_ddl.sql
│   │   ├── gold_definicao_dashboard.pdf
│   │   ├── gold_mer_der_dld.pdf
│   │   └── mnemonico.md
│   │
│   ├── raw
│   │   ├── raw_analysis.ipynb
│   │   ├── raw_data_dictionary.md
│   │   ├── raw_ddl.sql
│   │   ├── raw_mer_der_dld.pdf
│   │   ├── raw_metadados.md
│   │   └── stage
│   │       ├── airlines.csv
│   │       ├── airports.csv
│   │       ├── flights_part_01.csv
│   │       ├── flights_part_02.csv
│   │       ├── flights_part_03.csv
│   │       ├── flights_part_04.csv
│   │       ├── flights_part_05.csv
│   │       ├── flights_part_06.csv
│   │       ├── flights_part_07.csv
│   │       ├── flights_part_08.csv
│   │       ├── flights_part_09.csv
│   │       └── flights_part_10.csv
│   │
│   └── silver
│       ├── silver_analysis.ipynb
│       ├── silver_consultas.sql
│       ├── silver_data_dictionary.md
│       ├── silver_ddl.sql
│       └── silver_mer_der_dld.pdf
│
├── docker
│   ├── airflow
│   │   └── Dockerfile
│   └── jupyter
│       └── Dockerfile.transformer
│
├── docker-compose.yaml
├── pyproject.toml
└── transformer
    ├── __init__.py
    ├── dbt
    │   ├── .dbt
    │   │   └── profiles.yml
    │   ├── dbt_project.yml
    │   │
    │   ├── macros
    │   │   ├── generate_schema_name.sql
    │   │   ├── parse_int.sql
    │   │   ├── tests
    │   │   │   └── quality_gates.sql
    │   │   └── time_utils.sql
    │   │
    │   └── models
    │       ├── gold
    │       │   ├── dim_air.sql
    │       │   ├── dim_apt.sql
    │       │   ├── dim_dat.sql
    │       │   ├── fat_flt.sql
    │       │   └── gold_sources.yml
    │       ├── raw
    │       │   ├── raw_airlines.sql
    │       │   ├── raw_airports.sql
    │       │   ├── raw_flights.sql
    │       │   └── raw_sources.yml
    │       └── silver
    │           ├── silver_flights.sql
    │           └── silver_sources.yml
    │
    ├── etl_raw_to_silver.ipynb
    ├── etl_silver_to_gold.ipynb
    │
    └── utils
        ├── __init__.py
        ├── file_io.py
        ├── helpers.py
        ├── logger.py
        ├── metadata_collector.py
        ├── postgre_helpers.py
        ├── quality_gates_gold.py
        ├── quality_gates_raw.py
        ├── quality_gates_silver_aggregated.py
        ├── quality_gates_silver_base.py
        ├── quality_gates_silver_flights.py
        └── spark_helpers.py
```

---

## Pipeline

A orquestração é realizada pela DAG:

```
pl_raw_to_gold.py
```

Ela executa, de forma sequencial, dois notebooks ETL utilizando Papermill dentro do container `data_transformer` e quartro tarefas relativas ao dbt dentro do container `dbt_runner`.

### Banco de Dados (DW)

O PostgreSQL inicia automaticamente com:

- Schema `dbt_raw`;
- Schema `dbt_silver`;
- Schema `dbt_gold`;
- Schema `silver`;
- Schema `gold`.

Os scripts SQL de criação de tabelas estão localizados na `data-layer`.

As cargas das tabelas da `silver_*`, `gold_*` e do `dbt_*` são realizadas diretamente pelos notebooks ETL e com `BashOperator` do Airflow.

### Qualidade de Dados

O projeto implementa testes automáticos para as camadas silver e gold. Os validadores estão em:

```bash
transformer/utils/quality_gates_*.py
```

---

## Como executar o projeto

### 0. Pré-requisitos

Antes de começar, garanta que você tenha as seguintes ferramentas instaladas:

- [**Docker**](https://docs.docker.com/get-docker/)
- [**Docker Compose**](https://docs.docker.com/compose/install/)
- [**Python 3**](https://www.python.org/downloads/)

> **Obs.: É recomendado ter no mínimo 10GB de armazenamento livres.**

### 1. Clone o repositório

```bash
git clone https://github.com/phmelosilva/Flight-Delays-Data-Pipeline-and-Analysis.git
cd Flight-Delays-Data-Pipeline-and-Analysis
```

### 2. Gere as chaves de segurança

O Airflow requer uma chave de criptografia (Fernet Key) e uma chave secreta (JWT Secret). Execute os comandos abaixo no seu terminal para gerá-las.

```bash
# Gerar a Fernet Key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Gerar a JWT Secret
python3 -c "import secrets; print(secrets.token_hex(16))"
```

Salve os dois valores gerados.

### 3. Configure o arquivo `.env`

Primeiro, copie o arquivo `.env.example` de exemplo para criar seu arquivo de configuração local.

```bash
cp .env.example .env
```

Altere o arquivo `.env` na raiz do projeto com os valores de chave gerado anteriormente. Um exemplo é mostrado a seguir.

```env
...

# Airflow Commons
AIRFLOW__CORE__FERNET_KEY=''                <-- COLOQUE ENTRE ASPAS SIMPLES SUA CHAVE FERNET
AIRFLOW__API_AUTH__JWT_SECRET=              <-- COLOQUE DEPOIS DO '=' SUA CHAVE JWT

...
```

---

### 4. Suba o ambiente

Com a engine do Docker iniciado, suba todos os serviços com um único comando no terminal:

```bash
docker compose up --build -d
```

A primeira execução pode levar alguns minutos.

---

### 5. Acesse o Airflow

- **URL:** [http://localhost:8080](http://localhost:8080)
- **Usuário:** `airflow` (ou definida no .env)
- **Senha:** `airflow` (ou definida no .env)

---

### 6. Execute a pipeline

1. No Airflow, localize a DAG `pl_raw_to_gold`.
2. Ative a DAG.
3. Execute manualmente a dag.

Os notebooks executados via Papermill serão armazenados em:

```bash
transformer/output/<data_da_execução>/
```

---
