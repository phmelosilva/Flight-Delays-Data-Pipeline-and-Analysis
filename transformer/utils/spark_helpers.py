import os
import psycopg2
from typing import Optional, Dict, List
from pyspark.sql import SparkSession, DataFrame
from transformer.utils.logger import get_logger

log = get_logger("spark_helpers")

# Detecta ambiente com Airflow disponível
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False


def _resolve_postgres_connection(db_conn_id: str) -> tuple[str, str, str]:
    """
    Resolve parâmetros de conexão com o PostgreSQL tanto no modo Airflow
    quanto no modo standalone.

    Args:
        db_conn_id (str): ID da conexão definida no Airflow (ignorado fora dele).

    Returns:
        tuple[str, str, str]: (jdbc_url, username, password)
    """
    if AIRFLOW_AVAILABLE:
        hook = PostgresHook(postgres_conn_id=db_conn_id)
        conn = hook.get_connection(db_conn_id)

        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        user = conn.login
        password = conn.password

        return jdbc_url, user, password

    # Execução fora do Airflow
    log.warning("[WARN] Airflow indisponível, usando variáveis de ambiente para conexão PostgreSQL.")

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'postgres')}"
    )
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    return jdbc_url, user, password


def read_from_postgres(
    spark: SparkSession,
    db_conn_id: str,
    table_name: str,
    columns: Optional[List[str]] = None,
    predicates: Optional[List[str]] = None,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """
    Lê uma tabela PostgreSQL via JDBC e retorna um DataFrame Spark.

    Suporta:
        - projeção de colunas (SELECT);
        - leitura particionada (predicates);
        - fallback automático Airflow (standalone).

    Args:
        spark (SparkSession): Sessão Spark ativa.
        db_conn_id (str): ID da conexão no Airflow.
        table_name (str): Nome completo da tabela (schema.tabela).
        columns (list, opcional): Lista de colunas projetadas.
        predicates (list, opcional): Lista de predicados para leitura paralela.
        num_partitions (int, opcional): Número de partições paralelas.

    Returns:
        DataFrame: DataFrame resultante da leitura JDBC.

    Raises:
        Exception: Se ocorrerem falhas de conexão ou leitura.
    """
    try:
        log.info(f"[READ] Iniciando leitura de '{table_name}'.")

        jdbc_url, user, password = _resolve_postgres_connection(db_conn_id)

        connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

        # Caso seja necessário projetar colunas
        if columns:
            projection = ", ".join(columns)
            table_or_query = f"(SELECT {projection} FROM {table_name}) AS subq"
        else:
            table_or_query = table_name

        # Leitura particionada (quando disponível)
        if predicates and num_partitions:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_or_query,
                predicates=predicates,
                numPartitions=num_partitions,
                properties=connection_properties,
            )
        else:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_or_query,
                properties=connection_properties,
            )

        log.info(f"[READ] Leitura concluída: '{table_name}'. Linhas: {df.count()}")

        return df

    except Exception as e:
        log.error(f"[ERROR] Falha ao ler PostgreSQL ({table_name}): {e}")
        raise


def load_to_postgres(
    df: DataFrame,
    db_conn_id: str,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """
    Carrega um DataFrame para o PostgreSQL via JDBC.

    Em modo "overwrite":
        - executa TRUNCATE TABLE ... CASCADE;
        - escreve usando modo append para evitar recriação de tabela.

    Args:
        df (DataFrame): Dados Spark a carregar.
        db_conn_id (str): Conexão Airflow (ou variáveis de ambiente).
        table_name (str): Nome da tabela (schema.tabela).
        mode (str): 'overwrite' ou 'append'.

    Raises:
        Exception: Para erros de conexão ou escrita.
    """
    try:
        jdbc_url, user, password = _resolve_postgres_connection(db_conn_id)

        connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

        if mode == "overwrite":
            schema, table = table_name.split(".")
            log.info(f"[LOAD] Limpando tabela '{schema}.{table}' (TRUNCATE).")

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

        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=connection_properties,
        )

        log.info(f"[LOAD] Carga concluída em '{table_name}' (modo={mode}).")

    except Exception as e:
        log.error(f"[ERROR] Falha ao carregar PostgreSQL ({table_name}): {e}")
        raise


def get_spark_session(
    app_name: str,
    master: str = "local[*]",
    additional_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Cria uma SparkSession com configurações otimizadas para:
        - ETL local;
        - integração JDBC;
        - redução de logs desnecessários.

    Args:
        app_name (str): Nome da aplicação.
        master (str): Configuração do cluster (local[*], k8s, etc).
        additional_configs (dict, opcional): Configurações extras.

    Returns:
        SparkSession: Sessão Spark configurada.

    Raises:
        Exception: Caso ocorra falha na criação da sessão.
    """
    try:
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "2g")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.sql.shuffle.partitions", "30")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.sql.codegen.wholeStage", "true")
            .config("spark.sql.codegen.splitExpressions", "true")
            .config("spark.sql.codegen.maxFields", "150")
            .config("spark.sql.debug.maxToStringFields", "50")
            .config("spark.executor.extraJavaOptions", "-Xlog:disable")
            .config("spark.driver.extraJavaOptions", "-Xlog:disable")
        )

        # Configurações externas opcionais
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # Ajuste nos logs Java/Scala
        log4j = spark._jvm.org.apache.log4j
        log4j.LogManager.getLogger("org.codehaus.janino").setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger("org.codehaus.commons.compiler").setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger("org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator").setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger("org.apache.spark.sql.execution").setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger("org.apache.spark.sql.execution.WholeStageCodegenExec").setLevel(log4j.Level.ERROR)

        log.info(f"[INFO] SparkSession criada: '{app_name}' (master={master}).")

        return spark

    except Exception as e:
        log.error(f"[ERROR] Falha ao criar SparkSession '{app_name}': {e}.")
        raise
