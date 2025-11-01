import os
from airflow.decorators import dag, task
from pendulum import datetime, duration
from transformer.utils.spark_helpers import get_spark_session, save_df_as_parquet_file
from transformer.utils.file_io import check_files_in_folder, move_files, delete_files
from transformer.landing.reassemble_chunks import reassemble_chunks


@dag(
    dag_id="landing_stage_to_bronze",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["landing", "bronze", "spark"],
    doc_md="""
    ---------------------------------[ INFO: ]---------------------------------

        Esta DAG orquestra a ingestão de dados da área de stage para a
        camada Bronze. O processo segue 5 etapas:
            1. Verifica: Verifica a presença dos arquivos csv na stage.
            2. Unifica: Os chunks 'flights_part_*.csv' são unificados em um único arquivo parquet.
            3. Converte: Converte todos os demais csv's para parquet.
            4. Move: Transfere os arquivos Parquet para a camada bronze.
            5. Limpa: Deleta todos os arquivos (csv's originais e parquets
            temporários) da stage.

    ---------------------------------------------------------------------------
    """,
)
def landing_stage_to_bronze_dag():

    # Parâmetros globais
    STAGE_PATH = os.getenv("DATA_LAYER_STAGE_PATH", "/opt/airflow/data-layer/stage")
    BRONZE_PATH = os.getenv("DATA_LAYER_BRONZE_PATH", "/opt/airflow/data-layer/bronze")
    SPARK_CORES = os.getenv("SPARK_CORES", "*")

    # Configurações globais para Spark
    spark_conf = {
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem"
    }

    @task
    def check_files_in_stage() -> list[str]:
        """
        Verifica se há arquivos csv disponíveis na stage.
        """
        print(f"[INFO] Verificando arquivos csv em: {STAGE_PATH}.")
        files = check_files_in_folder(folder_path=STAGE_PATH, file_pattern="*.csv")
        print(f"[INFO] {len(files)} arquivo(s) encontrados.")
        return files

    @task
    def unify_flight_chunks(files_in_stage: list[str]) -> str:
        """
        Unifica chunks de voo em um único arquivo parquet.
        """
        spark = get_spark_session(
            app_name="UnifyFlightsChunks",
            master=f"local[{SPARK_CORES}]",
            additional_configs=spark_conf,
        )
        try:
            chunk_files = [f for f in files_in_stage if "flights_part" in f]
            if not chunk_files:
                print("[WARN] Nenhum chunk encontrado.")
                return ""

            print(f"[INFO] Unificando {len(chunk_files)} chunks.")
            df = reassemble_chunks(spark, chunk_files)

            parquet_path = save_df_as_parquet_file(
                df=df,
                dest_path=STAGE_PATH,
                filename="flights.parquet",
                single_file=True,
            )
            print(f"[INFO] Arquivo consolidado: {parquet_path}.")
            return parquet_path
        except Exception as e:
            print(f"[ERRO] Falha ao unificar chunks: {e}.")
            raise
        finally:
            spark.stop()
            print("[INFO] Sessão Spark encerrada.")

    @task
    def convert_csv_to_parquet(files_in_stage: list[str]) -> list[str]:
        """
        Converte arquivos csv não relacionados a chunks para parquet.
        """
        other_csvs = [f for f in files_in_stage if "flights_part" not in f]
        if not other_csvs:
            print("[WARN] Nenhum arquivo csv para conversão.")
            return []

        spark = get_spark_session(
            app_name="ConvertCsvToParquet",
            master=f"local[{SPARK_CORES}]",
            additional_configs=spark_conf,
        )

        new_files = []
        try:
            for csv_file in other_csvs:
                print(f"[INFO] Convertendo {csv_file}.")
                df = spark.read.csv(csv_file, header=True, inferSchema=True)

                base_name = os.path.basename(csv_file).replace(".csv", ".parquet")
                parquet_file = save_df_as_parquet_file(df, dest_path=STAGE_PATH, filename=base_name)
                new_files.append(parquet_file)

            print(f"[SUCCESS] {len(new_files)} arquivo(s) convertidos para parquet.")
            return new_files
        except Exception as e:
            print(f"[ERRO] Falha durante conversão: {e}.")
            raise
        finally:
            spark.stop()
            print("[INFO] Sessão Spark encerrada.")

    @task
    def move_files_to_bronze(unified_flight_file: str, parquet_files: list[str]) -> None:
        """
        Move arquivos gerados da stage para bronze.
        """
        files_to_move = list(parquet_files)
        if unified_flight_file:
            files_to_move.append(unified_flight_file)

        if not files_to_move:
            print("[WARN] Nenhum arquivo parquet disponível.")
            return

        spark = get_spark_session("MoveToBronze", master="local[1]", additional_configs=spark_conf)

        processing_date = datetime.now().strftime("%Y-%m-%d")

        try:
            move_files(
                spark=spark,
                source_files=files_to_move,
                base_dest_path=BRONZE_PATH,
                processing_date=processing_date,
            )
            print(f"[SUCCESS] Arquivos movidos para bronze ({processing_date}).")
        except Exception as e:
            print(f"[ERRO] Falha ao mover arquivos: {e}.")
            raise
        finally:
            spark.stop()
            print("[INFO] Sessão Spark encerrada.")

    @task
    def cleanup_stage(initial_files: list[str],
                      unified_flight_file: str,
                      other_parquets: list[str]
    ) -> None:
        """
        Remove arquivos csv e parquet temporários da Stage.
        """
        files_to_delete = initial_files + other_parquets
        if unified_flight_file:
            files_to_delete.append(unified_flight_file)

        if not files_to_delete:
            print("[WARN] Nenhum arquivo para remover.")
            return

        spark = get_spark_session("CleanUpStage", master="local[1]", additional_configs=spark_conf)

        try:
            delete_files(spark, files_to_delete)
            print(f"[SUCCESS] {len(files_to_delete)} arquivo(s) removido(s) da stage.")
        except Exception as e:
            print(f"[ERRO] Falha durante limpeza da stage: {e}.")
            raise
        finally:
            spark.stop()
            print("[INFO] Sessão Spark encerrada.")

    # Orquestração
    check_task_instance   = check_files_in_stage()
    unify_task_instance   = unify_flight_chunks(check_task_instance)
    convert_task_instance = convert_csv_to_parquet(check_task_instance)
    move_task_instance    = move_files_to_bronze(unify_task_instance, convert_task_instance)
    cleanup_task_instance = cleanup_stage(check_task_instance, unify_task_instance, convert_task_instance)

    [unify_task_instance, convert_task_instance] >> move_task_instance >> cleanup_task_instance

landing_stage_to_bronze_dag()
