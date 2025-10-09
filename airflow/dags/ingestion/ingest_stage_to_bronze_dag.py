from airflow.decorators import dag, task
from pendulum import datetime, duration
from pyspark.sql import SparkSession
from pipelines.utils.check_files_in_folder import check_files_in_folder
from pipelines.ingestion.reassemble_chunks import reassemble_chunks
from pipelines.utils.spark_processing import save_df_as_parquet_file
from pipelines.utils.file_management import move_files, delete_files


@dag(
    dag_id="ingest_stage_to_bronze",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["ingestion", "bronze", "stage"],
    doc_md="""
    ---------------------------------[ INFO: ]---------------------------------

        DAG de Ingestão da Stage para Bronze

        Esta DAG orquestra a ingestão de dados da área de stage para a
        camada Bronze. O processo segue 4 etapas sequenciais:
        1. Verifica: Verifica a presença dos arquivos CSV na stage.
        2. Unifica: Junta os chunks 'flights_part_*.csv' em um único arquivo
        'parquet' na stage.
        3. Move: Transfere os arquivos consolidados para a camada bronze.
        4. Limpa: Deleta todos os arquivos da stage.

    ---------------------------------------------------------------------------
    """,
)
def ingest_stage_to_bronze_dag():

    # Configura o Spark para não criar os checksum nas pastas.
    spark_conf = {
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem"
    }

    @task
    def check_files_in_folder_task(stage_path: str) -> list[str]:
        """
        Verifica se há arquivos CSV disponíveis na Stage.
        """
        return check_files_in_folder(folder_path=stage_path, file_pattern="*.csv")

    @task
    def unify_chunks_task(files_in_stage: list[str], stage_path: str, spark_cores: str) -> str:
        """
        Unifica os arquivos de chunk (`flights_part_*.csv`) em um único arquivo parquet.
        """
        spark = SparkSession.builder \
            .appName("UnifyFlightsChunks") \
            .master(f"local[{spark_cores}]") \
            .config(map=spark_conf) \
            .getOrCreate()
            
        try:
            flight_chunks = [f for f in files_in_stage if "flights_part" in f]
            flights_df = reassemble_chunks(spark, flight_chunks)
            
            new_filename = "flights.parquet"
            save_df_as_parquet_file(flights_df, dest_path=stage_path, filename=new_filename)

            return f"{stage_path}/{new_filename}"
        
        finally:
            spark.stop()

    @task
    def move_files_to_bronze_task(initial_files: list[str], unified_file: str, bronze_path: str, processing_date: str):
        """
        Move os arquivos finais da Stage para a camada Bronze.
        """
        spark = SparkSession.builder \
            .appName("MoveToBronze") \
            .master("local[1]") \
            .config(map=spark_conf) \
            .getOrCreate()
        try:
            files_to_move = [f for f in initial_files if "flights_part" not in f]
            files_to_move.append(unified_file)

            move_files(
                spark=spark,
                source_files=files_to_move,
                base_dest_path=bronze_path,
                processing_date=processing_date
            )

        finally:
            spark.stop()

    @task
    def clean_up_stage_task(initial_files: list[str], unified_file: str):
        """
        Remove todos os arquivos da Stage após a movimentação para Bronze.
        """
        spark = SparkSession.builder \
            .appName("CleanUpStage") \
            .master("local[1]") \
            .config(map=spark_conf) \
            .getOrCreate()
        try:
            files_to_delete = initial_files + [unified_file]
            delete_files(spark, files_to_delete)

        finally:
            spark.stop()

    initial_file_list = check_files_in_folder_task(
        stage_path="{{ var.value.datalake_stage_path }}"
    )
    
    unified_file_path = unify_chunks_task(
        files_in_stage=initial_file_list,
        stage_path="{{ var.value.datalake_stage_path }}",
        spark_cores="{{ var.value.airflow_spark_cores | default('4') }}"
    )
    
    move_task_instance = move_files_to_bronze_task(
        initial_files=initial_file_list,
        unified_file=unified_file_path,
        bronze_path="{{ var.value.datalake_bronze_path }}",
        processing_date="{{ data_interval_start.in_timezone('America/Sao_Paulo').format('YYYY-MM-DD') }}"
    )
    
    cleanup_task_instance = clean_up_stage_task(
        initial_files=initial_file_list, 
        unified_file=unified_file_path
    )

    unified_file_path >> move_task_instance >> cleanup_task_instance

ingest_stage_to_bronze_dag()
