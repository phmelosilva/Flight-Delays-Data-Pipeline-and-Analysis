# Flight Delays Data Pipeline and Analysis

Este é um projeto completo de engenharia de dados para a disciplina de **Sistemas de Bancos de Dados 2** que demonstra a construção de um pipeline de ponta a ponta, desde a ingestão de dados brutos até a disponibilização de insights por meio de ferramentas de BI. Utilizando [dados de voos de 2015 nos EUA](https://www.kaggle.com/datasets/usdot/flight-delays), o projeto implementa a **Arquitetura Medallion** (_Bronze_, _Silver_ e _Gold_) para processar, limpar e agregar os dados de forma incremental e confiável em um **Data Lake**.

O principal objetivo é criar um Data Lake robusto e automatizado, onde cada etapa do processo é orquestrada, testável e reprodutível, entregando dados de alta qualidade para consumo por ferramentas de **Business Intelligence**.

---

## Tecnologias utilizadas

- **Orquestração:** Apache Airflow;
- **Conteinerização:** Docker & Docker Compose;
- **Banco de Dados (Metadados do Airflow e Data Warehouse):** PostgreSQL 16;
- **Análise e Processamento de Dados:** Matplotlib, Pandas, PySpark, Seaborn e Scikit-learn;
- **Visualização:** Microsoft PowerBI e Tableau (Provisórios);
- **Linguagens:** Python, SQL & Bash.

## Estrutura do projeto

```
.
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── README.md
├── requirements.txt
├── setup.py
│
├── airflow
│   ├── config
│   ├── dags
│   │   ├── ingestion
│   │   ├── modeling
│   │   ├── setup
│   │   ├── stage
│   │   └── transformation
│   ├── logs
│   └── plugins
│
├── datalake
│   ├── stage
│   ├── bronze
│   ├── silver
│   └── gold
│
├── docs
│   └── data_dictionary
│       ├── bronze
│       ├── silver
│       └── gold
│
├── notebooks
│   ├── bronze_analysis
│   └── silver_analysis
│
├── pipelines
│   ├── ingestion
│   ├── modeling
│   ├── setup
│   ├── stage
│   └── transformation
│
└── tests
```

## Como executar o projeto

Siga os passos abaixo para configurar e executar o projeto em seu ambiente local.

### Pré-requisitos

Antes de começar, garanta que você tenha as seguintes ferramentas instaladas:

- [**Docker**](https://docs.docker.com/get-docker/)
- [**Docker Compose**](https://docs.docker.com/compose/install/)
- [**Python 3**](https://www.python.org/downloads/)

> **Obs.: É necessário ter no mínimo 5GB de armazenamento livres.**

### 1\. Clone o repositório

```bash
git clone https://github.com/phmelosilva/Flight-Delays-Data-Pipeline-and-Analysis.git

cd Flight-Delays-Data-Pipeline-and-Analysis
```

### 2\. Gere as chaves de segurança

O Airflow requer uma chave de criptografia (Fernet Key) e uma chave secreta (JWT Secret). Execute os comandos abaixo no seu terminal para gerá-las.

```bash
# Gerar a Fernet Key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Gerar a JWT Secret
python3 -c "import secrets; print(secrets.token_hex(16))"
```

Salve os dois valores gerados em um bloco de notas.

### 3\. Configure as variáveis de ambiente

Primeiro, copie o arquivo `.env.example` de exemplo para criar seu arquivo de configuração local.

```bash
cp .env.example .env
```

Com um editor de texto, altere o arquivo `.env` na raiz do projeto com os valores de chave gerado anteriormente. Um exemplo é mostrado a seguir.

```env
...

# Airflow Commons
AIRFLOW__CORE__FERNET_KEY='COLOQUE AQUI ENTRE ASPAS SIMPLES SUA CHAVE FERNET'   <-- Altere aqui
AIRFLOW__API_AUTH__JWT_SECRET=COLOQUE DEPOIS DO '=' SUA CHAVE JWT               <-- Altere aqui

...
```

### 4\. Inicie o contêiner com os serviços

Com a engine do Docker iniciado, suba todos os serviços (Airflow, Scheduler, Banco de Dados, etc.) com um único comando no terminal:

```bash
docker compose up -d --build
```

O processo pode demorar na primeira execução, pois o Docker fará o download das imagens e depêndecias necessárias e inicializará os serviços.

### 5\. Acesse a Interface do Airflow

Após se certificar que todos os serviçoes estão prontos, a interface do Airflow estará disponível no seu navegador:

- **URL:** `http://localhost:8080`
- **Usuário:** `airflow` ou `usuário definido no .env`
- **Senha:** `airflow` ou `senha definida no .env`

### 6\. Ative e execute as DAGs

---

## Histórico de Versões

| Versão | Data       | Descrição                                                  | Autor(es)                                        | Revisor(es)                                      |
| ------ | ---------- | ---------------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------ |
| `1.0`  | 21/09/2025 | Criação inicial do README com dicionário de dados.         | [Júlia Takaki](https://github.com/juliatakaki)   | [Matheus Henrique](https://github.com/mathonaut) |
| `1.1`  | 24/09/2025 | Ajustes no README com explicação sobre camada Gold.        | [Pedro Henrique](https://github.com/phmelosilva) | [Matheus Henrique](https://github.com/mathonaut) |
| `1.2`  | 25/09/2025 | Reestrutura o README para refletir as mudanças no projeto. | [Matheus Henrique](https://github.com/mathonaut) | [Joao Schmitz](https://github.com/JoaoSchmitz)   |
