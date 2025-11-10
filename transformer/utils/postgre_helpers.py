# Depois: Revisar a necesidade desse helper

import os
import psycopg2
from transformer.utils.logger import get_logger

log = get_logger("utils.postgre_helpers")

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    AIRFLOW_AVAILABLE = False


def run_db_validation(db_conn_id: str, table_name: str, expected_count: int) -> None:
    """
    Valida a contagem de linhas de uma tabela PostgreSQL.Executa uma query 
    'SELECT COUNT(*)' na tabela informada e compara com o número esperado ('expected_count').

    Args:
        db_conn_id (str): ID de conexão configurado no Airflow (ex.: 'dw').
        table_name (str): Nome completo da tabela a ser validada.
        expected_count (int): Quantidade esperada de registros.

    Raises:
        ValueError: Se a contagem no banco não corresponder à esperada.
        ConnectionError: Se a conexão com o banco falhar.
        Exception: Para falhas inesperadas durante a execução da query.
    """
    log.info(f"[INFO] Validando contagem da tabela '{table_name}'.")

    try:
        # Airflow disponível
        if AIRFLOW_AVAILABLE:
            hook = PostgresHook(postgres_conn_id=db_conn_id)
            sql = f"SELECT COUNT(*) FROM {table_name};"
            db_count = hook.get_first(sql)[0]
            log.info("[INFO] Conexão via Airflow PostgresHook bem-sucedida.")
        else:
            # Modo standalone (debug/local)
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
            log.info("[INFO] Conexão via psycopg2 bem-sucedida.")

        # Validação
        log.info(
            f"[INFO] Esperado:{expected_count} | Encontrado:{db_count}"
        )

        if db_count != expected_count:
            raise ValueError(
                f"[ERROR] Divergência de contagem detectada."
                f"[ERROR] Esperado:{expected_count} | Encontrado:{db_count}"
            )

        log.info(f"[INFO] Validação da contagem concluída com sucesso para '{table_name}'.")

    except psycopg2.OperationalError as e:
        log.error(f"[ERROR] Falha de conexão ao banco de dados: {e}.")
        raise ConnectionError("[ERROR] Erro ao conectar-se ao banco PostgreSQL.") from e
    except ValueError:
        raise
    except Exception as e:
        log.error(f"[ERROR] Falha inesperada ao validar a tabela '{table_name}': {e}.")
        raise
