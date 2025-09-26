import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from airflow.hooks.base import BaseHook

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def create_schemas():
    """
    Essa função garante a existência dos schemas necessários no banco de dados.
    Ela se conecta ao banco de dados e executa o comando
    'CREATE SCHEMA IF NOT EXISTS' para crias os schemas 'bronze' e 'silver'.
    """

    try:
        schemas = ['bronze', 'silver']
        conn = BaseHook.get_connection('flights_db')
        connection_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        engine = create_engine(connection_uri)

        # Testa conexão com o banco.
        try:
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
            logging.info(f"Conexão com o banco '{conn.schema}' bem-sucedida.")
        except OperationalError as oe:
            logging.error(f"Falha na conectividade com o banco: {oe}")
            raise

        # Verifica e cria os schemas se necessário.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            for name in schemas:
                query = text(f"CREATE SCHEMA IF NOT EXISTS {name};")
                connection.execute(query)
                logging.info(f"[{conn.host}] Schema '{name}' criado ou existente.")
        
    except SQLAlchemyError as e:
        logging.error(f"Erro no SQLAlchemy ao criar schema: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        raise
    finally:
        engine.dispose()
