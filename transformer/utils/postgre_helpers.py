from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def run_db_validation(db_conn_id: str, table_name: str, expected_count: int):
    """
    Valida a contagem de linhas em uma tabela do PostgreSQL usando um Airflow Hook.
    """

    hook = PostgresHook(postgres_conn_id=db_conn_id)
    sql = f"SELECT COUNT(*) FROM {table_name};"
    db_count = hook.get_first(sql)[0]

    log.info(f"Validando contagem de linhas para a tabela {table_name}. Esperado: {expected_count}, Encontrado no DB: {db_count}")
    if db_count != expected_count:
        raise ValueError(f"Validação do banco falhou! Contagem de linhas diverge. Esperado: {expected_count}, Encontrado: {db_count}")

    log.info("Validação da contagem de linhas no banco de dados bem-sucedida.")
