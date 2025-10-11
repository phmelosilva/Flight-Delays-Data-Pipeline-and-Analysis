from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def get_spark_session(app_name: str, master: str = "local[*]", additional_configs: Optional[Dict[str, str]] = None) -> SparkSession:
    """
    Cria e retorna uma SparkSession com configurações flexíveis.
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    builder = builder.config(
        "spark.jars.packages", 
        "org.postgresql:postgresql:42.7.3"
    )

    if additional_configs:
        for key, value in additional_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()

def save_to_postgres(df: DataFrame, db_conn_id: str, table_name: str, mode: str = "overwrite"):
    """
    Salva um DataFrame no PostgreSQL via JDBC, usando uma conexão do Airflow
    e construindo a URL JDBC no formato correto.
    """
    hook = PostgresHook(postgres_conn_id=db_conn_id)
    conn = hook.get_connection(db_conn_id)

    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

    connection_properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver"
    }

    (df.write
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=connection_properties
        ))

    log.info(f"DataFrame salvo com sucesso na tabela {table_name} com o modo '{mode}'.")
