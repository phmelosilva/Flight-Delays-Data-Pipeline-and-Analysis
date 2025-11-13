from airflow.decorators import dag
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration


SILVER_PATH = "{{ var.value.data_layer_silver_path }}"
GOLD_PATH = "{{ var.value.data_layer_gold_path }}"
POSTGRES_CONN_ID = "{{ var.value.postgres_conn_id }}"
TRANSFORMER_SERVING_PATH = "{{ var.value.transformer_serving_path }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(seconds=15),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="02_etl_silver_to_gold",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["silver", "gold", "etl", "serving"],
    doc_md="""
    +--------------------------------------------------------------------------+
    |                                                                          |
    |                       DAG - Silver para Gold                             |
    |                                                                          |
    |   Esta DAG orquestra a modelagem e carga da camada gold, construindo o   |
    |   esquema estrela e carregando as tabelas finais no DW.                  |
    |                                                                          |
    |   O fluxo segue as seguintes etapas:                                     |
    |       1 - Move: Copia o arquivo 'flights_aggregated.parquet' da Silver   |
    |           para a Gold.                                                   |
    |       2 - Modela: Constrói as tabelas dimensão e fato (Star Schema).     |
    |       3 - Carrega: Insere as tabelas modeladas no PostgreSQL.            |
    |       4 - Limpa: Remove o arquivo temporário agregado da Gold.           |
    |       5 - Finaliza: Remove notebooks de output gerados pelo Papermill.   |
    |                                                                          |
    +--------------------------------------------------------------------------+
    """,
)
def etl_silver_to_gold_dag():

    create_output_dir = BashOperator(
        task_id="create_output_dir_task",
        bash_command=f"mkdir -p {TRANSFORMER_SERVING_PATH}/output",
        do_xcom_push=False,
    )

    move_task = PapermillOperator(
        task_id="move_file_silver_to_gold_task",
        input_nb=f"{TRANSFORMER_SERVING_PATH}/00_serving_move_file_silver_to_gold_job.ipynb",
        output_nb=f"{TRANSFORMER_SERVING_PATH}/output/00_serving_move_file_silver_to_gold_output.ipynb",
        parameters={
            "run_mode": "latest",
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
            "aggregated_name": "flights_aggregated.parquet",
        },
    )

    build_schema_task = PapermillOperator(
        task_id="build_gold_star_schema_task",
        input_nb=f"{TRANSFORMER_SERVING_PATH}/01_serving_build_gold_star_schema_job.ipynb",
        output_nb=f"{TRANSFORMER_SERVING_PATH}/output/01_serving_build_gold_star_schema_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "gold_path": GOLD_PATH,
            "aggregated_name": "flights_aggregated.parquet",
        },
    )

    load_postgres_task = PapermillOperator(
        task_id="load_gold_to_postgres_task",
        input_nb=f"{TRANSFORMER_SERVING_PATH}/02_serving_load_gold_to_postgres_job.ipynb",
        output_nb=f"{TRANSFORMER_SERVING_PATH}/output/02_serving_load_gold_to_postgres_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "gold_path": GOLD_PATH,
            "postgres_conn_id": POSTGRES_CONN_ID,
        },
    )

    cleanup_gold_task = PapermillOperator(
        task_id="cleanup_gold_task",
        input_nb=f"{TRANSFORMER_SERVING_PATH}/03_serving_cleanup_gold_job.ipynb",
        output_nb=f"{TRANSFORMER_SERVING_PATH}/output/03_serving_cleanup_gold_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "gold_path": GOLD_PATH,
        },
    )

    cleanup_outputs_task = BashOperator(
        task_id="cleanup_papermill_outputs_task",
        bash_command=(
            f"echo '[Gold][CleanupOutput] Limpando notebooks de output ' && "
            f"rm -f {TRANSFORMER_SERVING_PATH}/output/*_output.ipynb && "
            f"echo '[Gold][CleanupOutput] Limpeza dos outputs concluída.'"
        ),
        trigger_rule="all_success",
        do_xcom_push=False,
    )

    create_output_dir >> move_task >> build_schema_task >> load_postgres_task
    load_postgres_task >> cleanup_gold_task >> cleanup_outputs_task

etl_silver_to_gold_dag()
