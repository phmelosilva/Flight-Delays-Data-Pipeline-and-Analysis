from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def load_data_to_postgres(spark: SparkSession, file_path: str, file_format: str, db_schema: str, db_table: str):
    """
    Lê dados de um arquivo/pasta (Parquet ou CSV) usando Spark e os carrega
    em uma tabela do PostgreSQL. A tabela será sobrescrita.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        file_path (str): O caminho para o arquivo ou pasta de dados.
        file_format (str): O formato do arquivo ('parquet' ou 'csv').
        db_schema (str): O schema de destino no banco de dados.
        db_table (str): A tabela de destino no banco de dados.
    """
    log.info(f"Iniciando a leitura do caminho '{file_path}' no formato '{file_format}'...")
    
    try:
        read_options = {"header": "true"} if file_format == "csv" else {}
        df = spark.read.format(file_format).options(**read_options).load(file_path)

        log.info(f"Leitura concluída. Carregando para a tabela '{db_schema}.{db_table}'...")

        conn = BaseHook.get_connection('flights_db')
        
        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        connection_properties = {
            "user": conn.login,
            "password": conn.password,
            "driver": "org.postgresql.Driver"
        }

        (df.write.jdbc(
            url=jdbc_url,
            table=f'"{db_schema}"."{db_table}"',
            mode="overwrite",
            properties=connection_properties
        ))

        log.info(f"Carga para a tabela '{db_schema}.{db_table}' concluída com sucesso.")

    except Exception as e:
        log.error(f"Falha ao carregar dados para a tabela '{db_schema}.{db_table}': {e}")
        raise