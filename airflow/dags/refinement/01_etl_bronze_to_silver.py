from airflow.decorators import dag
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration


BRONZE_PATH = "{{ var.value.data_layer_bronze_path }}"
SILVER_PATH = "{{ var.value.data_layer_silver_path }}"
POSTGRES_CONN_ID = "{{ var.value.postgres_conn_id }}"
TRANSFORMER_REFINEMENT_PATH = "{{ var.value.transformer_refinement_path }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(seconds=15),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="01_etl_bronze_to_silver",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bronze", "silver", "etl", "refinement"],
    doc_md="""
    +--------------------------------------------------------------------------+
    |                                                                          |
    |                       DAG - Bronze para Silver                           |
    |                                                                          |
    |   Esta DAG orquestra a limpeza, padronização e consolidação dos dados    |
    |   provenientes da camada Bronze, gerando os datasets tratados na Silver. |
    |                                                                          |
    |   O fluxo segue as seguintes etapas:                                     |
    |       0 - Create Outputs: Cria pasta de outputs dos notebooks.
    |       1 - Airlines: Padroniza e valida o dataset de companhias aéreas.   |
    |       2 - Airports: Padroniza e corrige o dataset de aeroportos.         |
    |       3 - Flights: Limpa e normaliza o dataset de voos.                  |
    |       4 - Aggregate: Consolida os datasets em um único arquivo agregado. |
    |       5 - Load: Insere o dataset agregado no schema Silver do Postgres.  |
    |       6 - Cleanup Silver: Remove arquivos intermediários, mantendo       |
    |           apenas o 'flights_aggregated.parquet'.                         |
    |       7 - Cleanup Outputs: Remove notebooks de output do Papermill.      |
    |                                                                          |
    +--------------------------------------------------------------------------+
    """,
)
def etl_bronze_to_silver_dag():

    create_output_dir = BashOperator(
        task_id="00_create_output_dir_task",
        bash_command=f"mkdir -p {TRANSFORMER_REFINEMENT_PATH}/output",
        do_xcom_push=False,
    )

    airlines_task = PapermillOperator(
        task_id="01_refinement_airlines_transform_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/01_refinement_airlines_transform_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/01_refinement_airlines_transform_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
        },
    )

    airports_task = PapermillOperator(
        task_id="02_refinement_airports_transform_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/02_refinement_airports_transform_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/02_refinement_airports_transform_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
        },
    )

    flights_task = PapermillOperator(
        task_id="03_refinement_flights_transform_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/03_refinement_flights_transform_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/03_refinement_flights_transform_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
        },
    )

    aggregate_task = PapermillOperator(
        task_id="04_refinement_silver_aggregate_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/04_refinement_silver_aggregate_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/04_refinement_silver_aggregate_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "silver_path": SILVER_PATH,
        },
    )

    load_task = PapermillOperator(
        task_id="05_refinement_silver_load_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/05_refinement_silver_load_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/05_refinement_silver_load_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "silver_path": SILVER_PATH,
            "postgres_conn_id": POSTGRES_CONN_ID,
        },
    )

    cleanup_silver_task = PapermillOperator(
        task_id="06_refinement_silver_cleanup_task",
        input_nb=f"{TRANSFORMER_REFINEMENT_PATH}/06_refinement_silver_cleanup_job.ipynb",
        output_nb=f"{TRANSFORMER_REFINEMENT_PATH}/output/06_refinement_silver_cleanup_output.ipynb",
        parameters={
            "run_mode": "latest",
            "run_date": None,
            "silver_path": SILVER_PATH,
        },
    )

    cleanup_outputs_task = BashOperator(
        task_id="07_cleanup_papermill_outputs_task",
        bash_command=(
            f"echo '[Refinement][CleanupOutput] Limpando notebooks de output ' && "
            f"rm -f {TRANSFORMER_REFINEMENT_PATH}/output/*_output.ipynb && "
            f"echo '[Refinement][CleanupOutput] Limpeza dos outputs concluída.'"
        ),
        trigger_rule="all_success",
        do_xcom_push=False,
    )

    create_output_dir >> airlines_task >> airports_task >> flights_task
    flights_task >> aggregate_task >> load_task >> cleanup_silver_task >> cleanup_outputs_task

etl_bronze_to_silver_dag()
