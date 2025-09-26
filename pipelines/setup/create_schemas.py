from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def create_schemas():
    """
    Essa função garante a existência dos schemas necessários no banco de dados.
    Ela se conecta ao banco de dados e executa o comando
    'CREATE SCHEMA IF NOT EXISTS' para crias os schemas 'bronze', 'silver' e 'gold'.
    """

    schemas = ['bronze', 'silver', 'gold']
    conn = BaseHook.get_connection('flights_db')
    connection_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(connection_uri)

    try:
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        log.info(f"Conexão com o banco '{conn.schema}' bem-sucedida.")

        # Verifica e cria os schemas se necessário.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            for schema in schemas:
                query = text(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                connection.execute(query)
                log.info(f"Schema '{schema}' criado ou existente.")

    except OperationalError as oe:
        log.error(f"Falha na conectividade com o banco: {oe}")
        raise
    except SQLAlchemyError as se:
        log.error(f"Erro no SQLAlchemy ao criar o schema: {se}")
        raise
    except Exception as e:
        log.error(f"Erro: {e}")
        raise
    finally:
        engine.dispose()
