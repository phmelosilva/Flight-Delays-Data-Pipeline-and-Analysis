import os
import psycopg2
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from transformer.utils.logger import get_logger

log = get_logger("spark_helpers")

# Verifica se o Airflow está disponível
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False


def load_to_postgres(
    df: DataFrame,
    db_conn_id: str,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """
    Carrega um Dataframe para o PostgreSQL via JDBC.

    Em modo "overwrite", executa TRUNCATE TABLE ... CASCADE antes de fazer append,
    garantindo substituição total sem duplicação.

    Args:
        df (DataFrame): Dataframe Spark a ser carregado.
        db_conn_id (str): ID de conexão configurada no Airflow (ou ignorado fora dele).
        table_name (str): Nome completo da tabela destino (schema.tabela).
        mode (str): Modo de escrita Spark ('overwrite' ou 'append').

    Raises:
        Exception: Caso ocorra falha na carga ou conexão.
    """
    try:
        # Determina origem da conexão (Airflow ou local)
        if AIRFLOW_AVAILABLE:
            hook = PostgresHook(postgres_conn_id=db_conn_id)
            conn = hook.get_connection(db_conn_id)
            jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

            user, password = conn.login, conn.password
        else:
            log.warning("[WARN] Airflow indisponível, utilizando variáveis de ambiente para conexão PostgreSQL.")

            jdbc_url = (
                f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:"
                f"{os.getenv('DB_PORT', '5432')}/"
                f"{os.getenv('DB_NAME', 'postgres')}"
            )

            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "postgres")

        # Configurações de conexão JDBC
        connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

        # Modo overwrite (aplica 'TRUNCATE TABLE' antes do append)
        if mode == "overwrite":
            schema, table = table_name.split(".")

            log.info(f"[LOAD] Limpando tabela '{schema}.{table}'.")

            with psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                dbname=os.getenv("DB_NAME", "postgres"),
                user=user,
                password=password,
            ) as conn_pg:
                with conn_pg.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE;")
                    conn_pg.commit()
            mode = "append"

        # Escrita via Spark JDBC
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=connection_properties,
        )

        log.info(f"[LOAD] Dados carregados em '{table_name}' com sucesso (modo: {mode}).")

    except Exception as e:
        log.error(f"[ERROR] Falha ao carregar dados no PostgreSQL ({table_name}): {e}.")
        raise

def save_df_as_parquet_file(
    df: DataFrame,
    dest_path: str,
    filename: str,
    single_file: bool = False,
) -> str:
    """
    Salva um Dataframe Spark em formato Parquet.Caso `single_file=True`, 
    consolida o Dataframe em um único arquivo antes de salvar.

    Args:
        df (DataFrame): Dataframe Spark a ser salvo.
        dest_path (str): Caminho base do diretório de destino.
        filename (str): Nome do arquivo/parquet.
        single_file (bool): Se True, coalesce para único arquivo antes de salvar.

    Returns:
        str: Caminho completo do diretório salvo.

    Raises:
        IOError: Caso ocorra erro ao salvar o arquivo.
    """
    final_output_dir = f"{dest_path}/{filename}"

    # Criação de referência ao FileSystem Hadoop
    fs = df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(
        df.sparkSession._jsc.hadoopConfiguration()
    )
    HPath = df.sparkSession._jvm.org.apache.hadoop.fs.Path

    # Limpeza do diretório de saída, se já existir
    if fs.exists(HPath(final_output_dir)):
        try:
            if fs.delete(HPath(final_output_dir), True):
                log.info(f"[INFO] Diretório existente ({final_output_dir}) removido.")
            else:
                raise IOError(f"[ERROR] Falha ao limpar diretório existente.")
        except Exception as e:
            log.error(f"[ERROR] Erro ao limpar diretório '{final_output_dir}': {e}.")
            raise

    # Escrita do Dataframe em parquet
    try:
        # Consolida em único arquivo
        df_to_write = df.coalesce(1) if single_file else df
        df_to_write.write.mode("overwrite").parquet(final_output_dir)

        log.info(f"[INFO] Dataframe salvo com sucesso em '{final_output_dir}'.")

        return final_output_dir

    except Exception as e:
        log.error(f"[ERROR] Falha ao salvar Dataframe em '{final_output_dir}': {e}.")
        raise

def get_spark_session(
    app_name: str,
    master: str = "local[*]",
    additional_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada com o driver JDBC do PostgreSQL.
    Inclui automaticamente o pacote do driver PostgreSQL e permite adicionar
    configurações extras via `additional_configs`.

    Args:
        app_name (str): Nome da aplicação Spark.
        master (str): Configuração do cluster (padrão: 'local[*]').
        additional_configs (Optional[Dict[str, str]]): Configurações adicionais do Spark.

    Returns:
        SparkSession: Sessão Spark configurada e pronta para uso.

    Raises:
        Exception: Caso a sessão não possa ser criada.
    """
    try:
        # Construção da SparkSession
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        )

        # Aplicação de configurações adicionais
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)

        # Criação da sessão e configuração de logs
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        log.info(f"[INFO] SparkSession criada com sucesso: '{app_name}' (master={master}).")

        return spark

    except Exception as e:
        log.error(f"[ERROR] Falha ao criar SparkSession '{app_name}': {e}.")
        raise
