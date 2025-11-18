from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime, duration


# Caminhos
NOTEBOOK_BASE_PATH = "/opt/airflow/transformer"
OUTPUT_BASE_PATH = f"{NOTEBOOK_BASE_PATH}/output"
NB_STAGE_TO_BRONZE = f"{NOTEBOOK_BASE_PATH}/01_etl_stage_to_bronze.ipynb"
NB_BRONZE_TO_SILVER = f"{NOTEBOOK_BASE_PATH}/02_etl_bronze_to_silver.ipynb"
NB_SILVER_TO_GOLD = f"{NOTEBOOK_BASE_PATH}/03_etl_silver_to_gold.ipynb"

# Variáveis Airflow
STAGE_PATH = "{{ var.value.data_layer_stage_path }}"
BRONZE_PATH = "{{ var.value.data_layer_bronze_path }}"
SILVER_PATH = "{{ var.value.data_layer_silver_path }}"
GOLD_PATH = "{{ var.value.data_layer_gold_path }}"
POSTGRES_CONN_ID = "{{ var.value.postgres_conn_id }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(seconds=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="pl_stage_to_gold",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["stage", "bronze", "silver", "gold", "etl"],
    doc_md="""
    +--------------------------------------------------------------------------+
    |                                                                          |
    |                     DAG - Pipeline Completo                              |
    |                                                                          |
    |   Esta DAG orquestra o pipeline completo da área de Stage até a Gold,    |
    |   executando notebooks parametrizados via papermill dentro do container  |
    |   'data_transformer'.                                                    |
    |                                                                          |
    |   Etapas:                                                                |
    |       1. Stage -> Bronze: Ingestão na camada Bronze em formato Parquet;  |
    |       2. Bronze -> Silver: Limpeza, normalização e agregação;            |
    |       3. Silver -> Gold: Modelagem em esquema estrela e carga final no   |
    |          Data Warehouse.                                                 |
    |                                                                          |
    +--------------------------------------------------------------------------+
    """,
)
def pipeline_stage_to_gold():

    create_output_dir_task = BashOperator(
        task_id="create_output_dir_task",
        bash_command=f"mkdir -p {OUTPUT_BASE_PATH}/{{{{ ds }}}}",
        do_xcom_push=False,
    )

    stage_to_bronze_task = BashOperator(
        task_id="stage_to_bronze_task",
        bash_command=(
            "docker exec data_transformer "
            f"papermill {NB_STAGE_TO_BRONZE} "
            f"{OUTPUT_BASE_PATH}/{{{{ ds }}}}/01_etl_stage_to_bronze.ipynb "
            f"-p stage_path {STAGE_PATH} "
            f"-p bronze_path {BRONZE_PATH} "
            f"-p run_mode 'latest'"
        ),
        do_xcom_push=False,
    )

    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver_task",
        bash_command=(
            "docker exec data_transformer "
            f"papermill {NB_BRONZE_TO_SILVER} "
            f"{OUTPUT_BASE_PATH}/{{{{ ds }}}}/02_etl_bronze_to_silver.ipynb "
            f"-p run_mode 'latest' "
            f"-p run_date None "
            f"-p bronze_path {BRONZE_PATH} "
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
            f"{ OUTPUT_BASE_PATH }/{{{{ ds }}}}/03_etl_silver_to_gold.ipynb "
            f"-p run_mode 'latest' "
            f"-p run_date None "
            f"-p silver_path {SILVER_PATH} "
            f"-p gold_path {GOLD_PATH} "
            f"-p aggregated_name 'flights_aggregated.parquet' "
            f"-p postgres_conn_id {POSTGRES_CONN_ID}"
        ),
        do_xcom_push=False,
    )

    create_output_dir_task >> stage_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task

pipeline_stage_to_gold()
