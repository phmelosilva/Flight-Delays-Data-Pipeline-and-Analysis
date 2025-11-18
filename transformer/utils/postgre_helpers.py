import os
import psycopg2
from transformer.utils.logger import get_logger

log = get_logger("postgres_helpers")

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    AIRFLOW_AVAILABLE = False


def assert_table_rowcount(db_conn_id: str, table_name: str, expected_count: int) -> None:
    """
    Valida que a tabela PostgreSQL contém exatamente o número de registros esperado.

    Args:
        db_conn_id (str): ID de conexão configurado no Airflow.
        table_name (str): Nome completo da tabela (ex.: 'silver.flights_silver').
        expected_count (int): Quantidade esperada de tuplas após a carga.

    Raises:
        ValueError: Se a contagem da tabela for diferente de expected_count.
        ConnectionError: Se houver falha de conexão com o banco.
        Exception: Para erros inesperados.
    """
    log.info(f"[AssertRowCount] Validando contagem da tabela '{table_name}'. ")

    try:
        # Modo Airflow
        if AIRFLOW_AVAILABLE:
            hook = PostgresHook(postgres_conn_id=db_conn_id)
            sql = f"SELECT COUNT(*) FROM {table_name};"
            db_count = hook.get_first(sql)[0]
            log.info("[AssertRowCount] Conexão Airflow.PostgresHook e contagem realizadas.")

        # Modo Standalone
        else:
            conn_params = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": os.getenv("DB_PORT", "5432"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASSWORD", "postgres"),
                "dbname": os.getenv("DB_NAME", "postgres"),
            }

            conn = psycopg2.connect(**conn_params)
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                db_count = cur.fetchone()[0]
            conn.close()

            log.info("[AssertRowCount] Conexão psycopg2 e contagem realizadas.")

        log.info(f"[AssertRowCount] Tuplas esperadas: {expected_count:,} | Tuplas encontradas: {db_count:,}.")

        if db_count != expected_count:
            raise ValueError(f"[AssertRowCount][ERROR] Divergência de contagem na tabela '{table_name}'.")

        log.info(f"[AssertRowCount] Validação concluída com sucesso.")

    except psycopg2.OperationalError as e:
        log.error(f"[AssertRowCount][ERROR] Falha de conexão com o banco: {e}.")
        raise ConnectionError("Erro ao conectar-se ao PostgreSQL.") from e

    except ValueError:
        raise

    except Exception as e:
        log.error(f"[AssertRowCount][ERROR] Falha inesperada durante validação: {e}.")
        raise
