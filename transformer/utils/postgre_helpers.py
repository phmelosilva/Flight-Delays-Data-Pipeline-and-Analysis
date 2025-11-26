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
        table_name (str): Nome completo da tabela (ex.: 'silver.silver_flights').
        expected_count (int): Quantidade esperada de tuplas após a carga.

    Raises:
        ValueError: Se a contagem divergir.
        ConnectionError: Para falhas de conexão.
        Exception: Para erros inesperados.
    """
    log.info(f"[AssertRowCount] Validando contagem da tabela '{table_name}'.")

    try:
        # Se o Airflow estiver disponível, usa PostgresHook
        if AIRFLOW_AVAILABLE:
            hook = PostgresHook(postgres_conn_id=db_conn_id)
            sql = f"SELECT COUNT(*) FROM {table_name};"
            db_count = hook.get_first(sql)[0]

        # Execução standalone usando psycopg2
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

        log.info(f"[AssertRowCount] Esperado: {expected_count:,} | Encontrado: {db_count:,}")

        if db_count != expected_count:
            raise ValueError(
                f"Divergência de contagem na tabela '{table_name}'. "
                f"Esperado {expected_count:,}, encontrado {db_count:,}."
            )

        log.info("[AssertRowCount] Validação concluída com sucesso.")

    except psycopg2.OperationalError as e:
        log.error(f"[AssertRowCount] Falha de conexão com o banco: {e}.")
        raise ConnectionError("Erro ao conectar-se ao PostgreSQL.") from e

    except ValueError:
        raise

    except Exception as e:
        log.error(f"[AssertRowCount] Erro inesperado durante validação: {e}.")
        raise
