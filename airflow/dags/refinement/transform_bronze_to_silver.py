import glob
import os
from airflow.decorators import dag, task
from pendulum import datetime, duration
from transformer.utils.spark_helpers import get_spark_session, save_to_postgres
from transformer.utils.postgre_helpers import run_db_validation
from transformer.utils.quality_gates import run_quality_gates_on_df
from transformer.refinement import airlines_transformations, airports_transformations, flights_transformations


@dag(
    dag_id="transform_bronze_to_silver",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": duration(seconds=30),
        "postgres_conn_id": "dw_db"
    },
    tags=["transformation", "bronze", "silver"],
    doc_md="""
    ---------------------------[ INFO: ]---------------------------

        DAG de Transforamção da Bronze para Silver

        Esta DAG orquestra a transformação e limpeza dos dados da camada Bronze,
        salvando o resultado na camada Silver e carregando no Data Warehouse.
        O fluxo segue as seguintes etapas:
        1. Transformações Iniciais: Trata e valida `airlines`, `airports` e `flights`
           em paralelo.
        2. Agregação: Junta os arquivos tratados em uma única tabela denormalizada.
        3. Carga no DW: Carrega a tabela agregada no PostgreSQL.
        4. Validação Final: Verifica a consistência dos dados no banco.

    ---------------------------------------------------------------
    """,
)
def transform_bronze_to_silver_dag():

    @task
    def find_latest_bronze_partition_task(base_path: str) -> str:
        """Encontra o diretório de partição (YYYY-MM-DD) mais recente na camada Bronze."""
        pattern = os.path.join(base_path, "*-*-*")
        partitions = glob.glob(pattern)
        if not partitions:
            raise FileNotFoundError(f"Nenhuma partição de dados encontrada em {base_path}")
        latest_partition = max(partitions)
        return os.path.basename(latest_partition)

    @task
    def transform_bronze_airlines_task(latest_partition_date: str, bronze_path: str, silver_path: str) -> str:
        """Lê, transforma, valida e salva os dados de airlines."""
        spark = get_spark_session("TransformAirlines")
        try:
            source_path = f"{bronze_path}/{latest_partition_date}/PARQUET/airlines.parquet"
            dest_path = f"{silver_path}/{latest_partition_date}/PARQUET/airlines_silver.parquet"

            df = spark.read.parquet(source_path)
            transformed_df = airlines_transformations.transform_airlines(df)
            run_quality_gates_on_df(transformed_df, ["row_count_not_empty"])
            transformed_df.write.mode("overwrite").parquet(dest_path)
            return dest_path
        finally:
            spark.stop()

    @task
    def transform_bronze_airports_task(latest_partition_date: str, bronze_path: str, silver_path: str) -> str:
        """Lê, transforma, valida e salva os dados de airports."""
        spark = get_spark_session("TransformAirports")
        try:
            source_path = f"{bronze_path}/{latest_partition_date}/PARQUET/airports.parquet"
            dest_path = f"{silver_path}/{latest_partition_date}/PARQUET/airports_silver.parquet"
            
            df = spark.read.parquet(source_path)
            transformed_df = airports_transformations.transform_airports(df)
            run_quality_gates_on_df(transformed_df, ["row_count_not_empty"])
            transformed_df.write.mode("overwrite").parquet(dest_path)
            return dest_path
        finally:
            spark.stop()

    @task
    def transform_bronze_flights_task(latest_partition_date: str, bronze_path: str, silver_path: str) -> str:
        """Lê os dados de flights da partição mais recente, transforma e salva."""
        spark = get_spark_session("TransformFlights")
        try:
            source_path = f"{bronze_path}/{latest_partition_date}/PARQUET/flights.parquet"
            dest_path = f"{silver_path}/{latest_partition_date}/PARQUET/flights_pre_join.parquet"

            df = spark.read.parquet(source_path)
            transformed_df = flights_transformations.transform_flights(df)
            run_quality_gates_on_df(transformed_df, ["row_count_not_empty"])
            transformed_df.write.mode("overwrite").parquet(dest_path)
            return dest_path
        finally:
            spark.stop()

    @task
    def aggregate_df_task(flights_path: str, airlines_path: str, airports_path: str, silver_path: str) -> dict:
        """Lê os arquivos Silver, junta-os, aplica quality gates e salva o resultado final."""
        spark = get_spark_session("AggregateJoinSilver")
        try:
            latest_partition_date = os.path.basename(os.path.dirname(os.path.dirname(flights_path)))
            dest_path = f"{silver_path}/{latest_partition_date}/PARQUET/flights_aggregated.parquet"

            flights_df = spark.read.parquet(flights_path)
            airlines_df = spark.read.parquet(airlines_path)
            airports_df = spark.read.parquet(airports_path)

            aggregated_df = flights_transformations.create_aggregated_flights_df(flights_df, airlines_df, airports_df)

            aggregated_df.write.mode("overwrite").parquet(dest_path)
            return {"path": dest_path, "count": aggregated_df.count()}
        finally:
            spark.stop()

    @task
    def load_to_postgres_task(aggregation_result: dict, postgres_conn_id: str):
        """Carrega o arquivo Parquet final na tabela do PostgreSQL."""
        spark = get_spark_session("LoadToPostgres")
        try:
            df = spark.read.parquet(aggregation_result["path"])
            save_to_postgres(df, db_conn_id=postgres_conn_id, table_name="silver.flights_silver")
        finally:
            spark.stop()
        return aggregation_result["count"]

    @task
    def validate_db_load_task(expected_row_count: int, postgres_conn_id: str):
        """Verifica se a contagem de linhas no banco de dados corresponde ao esperado."""
        run_db_validation(
            db_conn_id=postgres_conn_id,
            table_name="silver.flights_silver",
            expected_count=expected_row_count
        )

    bronze_path_var = "{{ var.value.datalake_bronze_path }}"
    silver_path_var = "{{ var.value.datalake_silver_path }}"

    latest_partition = find_latest_bronze_partition_task(base_path=bronze_path_var)

    airlines_silver_path = transform_bronze_airlines_task(
        latest_partition_date=latest_partition, 
        bronze_path=bronze_path_var, 
        silver_path=silver_path_var
    )
    airports_silver_path = transform_bronze_airports_task(
        latest_partition_date=latest_partition,
        bronze_path=bronze_path_var,
        silver_path=silver_path_var
    )
    flights_silver_path = transform_bronze_flights_task(
        latest_partition_date=latest_partition,
        bronze_path=bronze_path_var,
        silver_path=silver_path_var
    )

    aggregation_result = aggregate_df_task(
        flights_path=flights_silver_path,
        airlines_path=airlines_silver_path,
        airports_path=airports_silver_path,
        silver_path=silver_path_var
    )

    written_row_count = load_to_postgres_task(aggregation_result=aggregation_result)

    validate_db_load_task(expected_row_count=written_row_count)

    [airlines_silver_path, airports_silver_path, flights_silver_path] >> aggregation_result

transform_bronze_to_silver_dag()
