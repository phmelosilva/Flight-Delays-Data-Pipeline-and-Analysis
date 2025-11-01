from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.utils.log.logging_mixin import LoggingMixin
    log = LoggingMixin().log
    AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    import logging
    log = logging.getLogger(__name__)
    AIRFLOW_AVAILABLE = False


def get_spark_session(
        app_name: str, 
        master: str = "local[*]", 
        additional_configs: Optional[Dict[str, str]] = None
    ) -> SparkSession:
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

def save_to_postgres(
        df: DataFrame, 
        db_conn_id: str, 
        table_name: str, 
        mode: str = "overwrite"
    ):
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

def save_df_as_parquet_file(
        df: DataFrame, 
        dest_path: str, 
        filename: str, 
        single_file: bool = False
    ) -> str:
    """
    Salva um DataFrame Spark em formato Parquet.

    Por padrão, os dados são salvos de forma particionada (vários arquivos part-*.parquet).
    Caso seja necessário gerar um único arquivo parquet consolidado, utilize o parâmetro
    `single_file=True`.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        dest_path (str): Diretório de destino.
        filename (str): Nome da subpasta onde os dados serão gravados.
        single_file (bool): Se True, força a escrita em apenas um arquivo parquet (coalesce=1).

    Returns:
        str: Caminho final em que o DataFrame foi salvo.

    Raises:
        IOError: Se ocorrer falha ao deletar diretório existente antes da escrita.
        Exception: Se ocorrer qualquer falha inesperada durante a escrita do Parquet.
    """
    final_output_dir = f"{dest_path}/{filename}"

    fs = df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(
        df.sparkSession._jsc.hadoopConfiguration()
    )
    Path = df.sparkSession._jvm.org.apache.hadoop.fs.Path

    if fs.exists(Path(final_output_dir)):
        if not fs.delete(Path(final_output_dir), True):
            raise IOError(f"Não foi possível limpar o diretório existente: {final_output_dir}")

    try:
        df_to_write = df.coalesce(1) if single_file else df
        df_to_write.write.mode("overwrite").parquet(final_output_dir)

        log.info(
            f"DataFrame salvo em formato parquet em '{final_output_dir}'."
        )

        return final_output_dir

    except Exception as e:
        log.error(f"Falha ao salvar DataFrame em '{final_output_dir}': {e}")
        raise
