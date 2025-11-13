from airflow.decorators import dag
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration


STAGE_PATH = "{{ var.value.data_layer_stage_path }}"
BRONZE_PATH = "{{ var.value.data_layer_bronze_path }}"
TRANSFORMER_LANDING_PATH = "{{ var.value.transformer_landing_path }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(seconds=15),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="00_etl_stage_to_bronze",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["stage", "bronze", "etl", "landing"],
    doc_md="""
    +--------------------------------------------------------------------------+
    |                                                                          |
    |                       DAG - Stage para Bronze                            |
    |                                                                          |
    |   Esta DAG orquestra a ingestão e consolidação dos dados da camada       |
    |   Stage para a Bronze.                                                   |
    |                                                                          |
    |   O fluxo segue as seguintes etapas:                                     |
    |       1 - Unifica: Consolida os chunks 'flights_part_*.csv' em um único  |
    |           arquivo Parquet dentro da Stage.                               |
    |       2 - Converte: Converte os demais arquivos CSV em formato Parquet.  |
    |       3 - Move: Transfere os arquivos Parquet da Stage para a Bronze,    |
    |           criando partições datadas.                                     |
    |       4 - Limpa: Remove os arquivos residuais da Stage (CSV e Parquet).  |
    |       5 - Finaliza: Remove os notebooks de output gerados pelo Papermill.|
    |                                                                          |
    +--------------------------------------------------------------------------+
    """,
)
def etl_stage_to_bronze_dag():

    # Criação da pasta de output para notebooks
    create_output_dir = BashOperator(
        task_id="00_create_output_dir_task",
        bash_command=f"mkdir -p {TRANSFORMER_LANDING_PATH}/output",
        do_xcom_push=False,
    )

    # Unificação dos chunks de voos
    unify_flight_chunks_task = PapermillOperator(
        task_id="01_unify_flight_chunks_task",
        input_nb=f"{TRANSFORMER_LANDING_PATH}/01_landing_unify_flight_chunks_job.ipynb",
        output_nb=f"{TRANSFORMER_LANDING_PATH}/output/01_landing_unify_flight_chunks_output.ipynb",
        parameters={
            "stage_path": STAGE_PATH,
            "bronze_path": BRONZE_PATH,
        },
    )

    # Conversão dos demais csvs em parquet
    convert_csv_to_parquet_task = PapermillOperator(
        task_id="02_convert_csv_to_parquet_task",
        input_nb=f"{TRANSFORMER_LANDING_PATH}/02_landing_convert_csv_to_parquet_job.ipynb",
        output_nb=f"{TRANSFORMER_LANDING_PATH}/output/02_landing_convert_csv_to_parquet_output.ipynb",
        parameters={
            "stage_path": STAGE_PATH,
        },
    )

    # Movimentação dos arquivos
    move_files_to_bronze_task = PapermillOperator(
        task_id="03_move_files_to_bronze_task",
        input_nb=f"{TRANSFORMER_LANDING_PATH}/03_landing_move_files_to_bronze_job.ipynb",
        output_nb=f"{TRANSFORMER_LANDING_PATH}/output/03_landing_move_files_to_bronze_output.ipynb",
        parameters={
            "stage_path": STAGE_PATH,
            "bronze_path": BRONZE_PATH,
        },
    )

    # Limpeza da Stage
    cleanup_stage_task = PapermillOperator(
        task_id="04_cleanup_stage_task",
        input_nb=f"{TRANSFORMER_LANDING_PATH}/04_landing_cleanup_stage_job.ipynb",
        output_nb=f"{TRANSFORMER_LANDING_PATH}/output/04_landing_cleanup_stage_output.ipynb",
        parameters={
            "stage_path": STAGE_PATH,
        },
    )

    # Limpeza dos outputs
    cleanup_outputs_task = BashOperator(
        task_id="05_cleanup_papermill_outputs_task",
        bash_command=(
            f"echo '[Landing][CleanupOutput] Limpando notebooks de output ' && "
            f"rm -f {TRANSFORMER_LANDING_PATH}/output/*_output.ipynb && "
            f"echo '[Landing][CleanupOutput] Limpeza dos outputs concluída.'"
        ),
        trigger_rule="all_success",
        do_xcom_push=False,
    )

    create_output_dir >> unify_flight_chunks_task >> convert_csv_to_parquet_task
    convert_csv_to_parquet_task >> move_files_to_bronze_task >> cleanup_stage_task >> cleanup_outputs_task

etl_stage_to_bronze_dag()
