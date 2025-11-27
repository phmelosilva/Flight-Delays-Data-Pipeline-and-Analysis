from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime, duration

# Caminhos dos Notebooks
NOTEBOOK_BASE_PATH = "/opt/airflow/transformer"
OUTPUT_BASE_PATH = f"{NOTEBOOK_BASE_PATH}/output"
NB_RAW_TO_SILVER = f"{NOTEBOOK_BASE_PATH}/etl_raw_to_silver.ipynb"
NB_SILVER_TO_GOLD = f"{NOTEBOOK_BASE_PATH}/etl_silver_to_gold.ipynb"

# Variáveis Airflow
STAGE_PATH = "{{ var.value.data_layer_stage_path }}"
RAW_PATH = "{{ var.value.data_layer_raw_path }}"
SILVER_PATH = "{{ var.value.data_layer_silver_path }}"
GOLD_PATH = "{{ var.value.data_layer_gold_path }}"
POSTGRES_CONN_ID = "{{ var.value.postgres_conn_id }}"

# Variáveis dbt
DBT_PROJECT_DIR = "/usr/app/dbt"
DBT_PROFILE_DIR = "/root/.dbt"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(seconds=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="pl_raw_to_gold",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["stage", "raw", "silver", "gold", "etl"],
    doc_md="""
    +--------------------------------------------------------------------------+
    |                                                                          |
    |                        DAG - Pipeline Completo                           |
    |                                                                          |
    |   Esta dag orquestra o pipeline completo da camada Raw até a Gold,       |
    |   executando notebooks parametrizados via Papermill dentro do container  |
    |   'data_transformer', enquanto realiza as mesmas etapas em um conjunto   |
    |   de dados reduzido utilizando o dbt dentro do container 'dbt_runner'.   |
    |                                                                          |
    |   Etapas:                                                                |
    |       1. Stage -> Raw: Ingestão dos dados em formato Parquet;            |
    |       2. Raw -> Silver: Limpeza, padronização e agregação;               |
    |       3. Silver -> Gold: Modelagem em esquema estrela.                   |
    |                                                                          |
    +--------------------------------------------------------------------------+
    """,
)
def pipeline_raw_to_gold():

    create_output_dir_task = BashOperator(
        task_id="create_output_dir_task",
        bash_command=f"mkdir -p {OUTPUT_BASE_PATH}/{{{{ ds }}}}",
        do_xcom_push=False,
    )

    # Tasks do Spark
    raw_to_silver_task = BashOperator(
        task_id="raw_to_silver_task",
        bash_command=(
            "docker exec data_transformer "
            f"papermill {NB_RAW_TO_SILVER} "
            f"{OUTPUT_BASE_PATH}/{{{{ ds }}}}/etl_raw_to_silver.ipynb "
            f"-p run_mode 'latest' "
            f"-p run_date None "
            f"-p stage_path {STAGE_PATH} "
            f"-p raw_path {RAW_PATH} "
            f"-p silver_path {SILVER_PATH} "
            f"-p postgres_conn_id {POSTGRES_CONN_ID}"
        ),
        do_xcom_push=False,
    )

    silver_to_gold_task = BashOperator(
        task_id="silver_to_gold_task",
        bash_command=(
            "docker exec data_transformer "
            f"papermill {NB_SILVER_TO_GOLD} "
            f"{OUTPUT_BASE_PATH}/{{{{ ds }}}}/etl_silver_to_gold.ipynb "
            f"-p run_mode 'latest' "
            f"-p run_date None "
            f"-p silver_path {SILVER_PATH} "
            f"-p gold_path {GOLD_PATH} "
            f"-p aggregated_name 'flights_aggregated.parquet' "
            f"-p postgres_conn_id {POSTGRES_CONN_ID}"
        ),
        do_xcom_push=False,
    )

    # Tasks do dbt
    dbt_run_raw_task = BashOperator(
        task_id="dbt_run_raw_task",
        bash_command=(
            f"docker exec dbt_runner "
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR} "
            "--select raw"
        ),
        do_xcom_push=False,
    )

    dbt_run_silver_task = BashOperator(
        task_id="dbt_run_silver_task",
        bash_command=(
            f"docker exec dbt_runner "
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR} "
            "--select silver"
        ),
        do_xcom_push=False,
    )

    dbt_run_gold_task = BashOperator(
        task_id="dbt_run_gold_task",
        bash_command=(
            f"docker exec dbt_runner "
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR} "
            "--select gold"
        ),
        do_xcom_push=False,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_task",
        bash_command=(
            f"docker exec dbt_runner "
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR}"
        ),
        do_xcom_push=False,
    )

    dbt_docs_generate_task = BashOperator(
        task_id="dbt_docs_generate_task",
        bash_command=(
            f"docker exec dbt_runner "
            f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILE_DIR}"
        ),
        do_xcom_push=False,
    )

    # Orquestração
    create_output_dir_task >> raw_to_silver_task

    raw_to_silver_task >> [silver_to_gold_task, dbt_run_raw_task]

    [silver_to_gold_task, dbt_run_raw_task] >> dbt_run_silver_task >> dbt_run_gold_task

    dbt_run_gold_task >> dbt_test_task >> dbt_docs_generate_task


pipeline_raw_to_gold()
