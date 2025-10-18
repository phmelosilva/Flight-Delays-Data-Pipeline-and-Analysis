from airflow.decorators import dag, task
from pendulum import datetime
from pyspark.sql import SparkSession
from transformer.ingestion.load_datalake_to_postgres import load_data_to_postgres

@dag(
    dag_id="load_silver_to_db_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # ATENÇÃO -> Será disparado por outro DAG no futuro, por enquanto manual
    catchup=False,
    tags=['ingestion', 'silver', 'postgres'],
    doc_md="""
    ### DAG de Carga da Silver para o Banco de Dados

    Este DAG lê os arquivos tratados da camada Silver do Data Lake e os carrega
    em uma única tabela no PostgreSQL para ser consumida pelo dbt.
    """,
)
def load_silver_to_db_dag():
    
    @task
    def load_silver_files_to_postgres_task(silver_path: str, processing_date: str):
        """
        Carrega os arquivos da camada Silver (Parquet) para uma tabela no PostgreSQL.
        """
        spark = SparkSession.builder \
            .appName("SilverToPostgres") \
            .master("local[2]") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
            .getOrCreate()
            
        try:
            # O caminho para os arquivos Parquet tratados na camada Silver
            # Assumindo que a transformação anterior salvou tudo em uma única pasta.
            source_data_path = f"{silver_path}/{processing_date}/flights_treated"
            
            load_data_to_postgres(
                spark=spark,
                file_path=source_data_path,
                file_format="parquet",
                db_schema="silver",
                db_table="flights_silver_table"
            )
            
        finally:
            spark.stop()

    # Criar variável AIRFLOW_VAR_DATALAKE_SILVER_PATH no .env
    silver_path_var = "{{ var.value.datalake_silver_path | default('./datalake/silver') }}"
    processing_date_var = "{{ data_interval_start.in_timezone('America/Sao_Paulo').format('YYYY-MM-DD') }}"

    load_silver_files_to_postgres_task(
        silver_path=silver_path_var,
        processing_date=processing_date_var
    )

load_silver_to_db_dag()