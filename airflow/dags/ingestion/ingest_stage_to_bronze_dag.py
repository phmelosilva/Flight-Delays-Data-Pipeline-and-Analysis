import os
from airflow.decorators import dag, task
from pendulum import datetime, duration
from pyspark.sql import SparkSession
from transformer.utils.check_files_in_folder import check_files_in_folder
from transformer.ingestion.reassemble_chunks import reassemble_chunks
from transformer.utils.spark_processing import save_df_as_parquet_file
from transformer.utils.file_management import move_files, delete_files

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
        2. Converte e Unifica: Converte todos os CSVs para Parquet. Os chunks
        'flights_part_*.csv' são unificados em um único arquivo Parquet, e
        os demais CSVs são convertidos individualmente.
        3. Move: Transfere os arquivos Parquet para a camada bronze.
        4. Limpa: Deleta todos os arquivos (CSVs originais e Parquets
        temporários) da stage.

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
            if not flight_chunks:
                return "" # Retorna string vazia se não houver chunks para unificar
                
            flights_df = reassemble_chunks(spark, flight_chunks)

            new_filename = "flights.parquet"
            save_df_as_parquet_file(flights_df, dest_path=stage_path, filename=new_filename)

            return f"{stage_path}/{new_filename}"

        finally:
            spark.stop()

    @task
    def convert_csv_to_parquet_task(files_in_stage: list[str], stage_path: str) -> list[str]:
        """
        Converte arquivos CSV (exceto chunks de voo) para o formato Parquet.
        """
        other_csvs = [f for f in files_in_stage if "flights_part" not in f]
        if not other_csvs:
            return [] # Retorna lista vazia se não houver outros CSVs

        spark = SparkSession.builder \
            .appName("ConvertCsvToParquet") \
            .master("local[1]") \
            .config(map=spark_conf) \
            .getOrCreate()
        
        new_parquet_files = []
        try:
            for csv_path in other_csvs:
                df = spark.read.csv(csv_path, header=True, inferSchema=True)
                
                base_filename = os.path.basename(csv_path)
                new_filename = base_filename.replace('.csv', '.parquet')
                
                save_df_as_parquet_file(df, dest_path=stage_path, filename=new_filename)
                new_parquet_files.append(f"{stage_path}/{new_filename}")

            return new_parquet_files
        finally:
            spark.stop()

    @task
    def move_files_to_bronze_task(
        unified_flight_file: str, 
        other_parquet_files: list[str], 
        bronze_path: str, 
        processing_date: str
    ):
        """
        Move todos os arquivos Parquet gerados da Stage para a camada Bronze.
        """
        files_to_move = other_parquet_files
        if unified_flight_file: # Adiciona o arquivo unificado apenas se ele foi criado
            files_to_move.append(unified_flight_file)

        if not files_to_move:
            print("Nenhum arquivo para mover.")
            return

        spark = SparkSession.builder \
            .appName("MoveToBronze") \
            .master("local[1]") \
            .config(map=spark_conf) \
            .getOrCreate()
        try:
            move_files(
                spark=spark,
                source_files=files_to_move,
                base_dest_path=bronze_path,
                processing_date=processing_date
            )
        finally:
            spark.stop()

    @task
    def clean_up_stage_task(
        initial_csv_files: list[str], 
        unified_flight_file: str, 
        other_parquet_files: list[str]
    ):
        """
        Remove todos os arquivos originais (CSV) e gerados (Parquet) da Stage.
        """
        files_to_delete = initial_csv_files + other_parquet_files
        if unified_flight_file:
            files_to_delete.append(unified_flight_file)
        
        if not files_to_delete:
            print("Nenhum arquivo para limpar.")
            return

        spark = SparkSession.builder \
            .appName("CleanUpStage") \
            .master("local[1]") \
            .config(map=spark_conf) \
            .getOrCreate()
        try:
            delete_files(spark, files_to_delete)
        finally:
            spark.stop()

    # --- Fluxo da DAG ---
    stage_path_var = "{{ var.value.datalake_stage_path }}"
    bronze_path_var = "{{ var.value.datalake_bronze_path }}"

    initial_file_list = check_files_in_folder_task(
        stage_path=stage_path_var
    )

    unified_file_path = unify_chunks_task(
        files_in_stage=initial_file_list,
        stage_path=stage_path_var,
        spark_cores="{{ var.value.airflow_spark_cores | default('4') }}"
    )

    other_parquet_paths = convert_csv_to_parquet_task(
        files_in_stage=initial_file_list,
        stage_path=stage_path_var
    )

    move_task_instance = move_files_to_bronze_task(
        unified_flight_file=unified_file_path,
        other_parquet_files=other_parquet_paths,
        bronze_path=bronze_path_var,
        processing_date="{{ data_interval_start.in_timezone('America/Sao_Paulo').format('YYYY-MM-DD') }}"
    )

    cleanup_task_instance = clean_up_stage_task(
        initial_csv_files=initial_file_list, 
        unified_flight_file=unified_file_path,
        other_parquet_files=other_parquet_paths
    )
    
    # Define as dependências
    [unified_file_path, other_parquet_paths] >> move_task_instance >> cleanup_task_instance

ingest_stage_to_bronze_dag()