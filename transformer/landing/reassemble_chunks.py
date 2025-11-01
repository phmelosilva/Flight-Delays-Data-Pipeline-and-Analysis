from pyspark.sql import SparkSession, DataFrame

try:
    from airflow.utils.log.logging_mixin import LoggingMixin
    log = LoggingMixin().log
    AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    import logging
    log = logging.getLogger(__name__)
    AIRFLOW_AVAILABLE = False

def reassemble_chunks(spark: SparkSession, chunk_files: list[str], header: bool = True) -> DataFrame:
    """
    Lê múltiplos chunks (arquivos CSV) e unifica em um único DataFrame Spark.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        chunk_files (list[str]): Lista de caminhos para os arquivos CSV a serem unificados.
        header (bool): Indica se os arquivos CSV possuem cabeçalho.

    Returns:
        DataFrame: DataFrame Spark contendo os dados unificados.

    Raises:
        ValueError: Se a lista de arquivos estiver vazia.
        FileNotFoundError: Se algum dos arquivos não for encontrado.
        IOError: Se ocorrer falha ao ler os arquivos CSV com o Spark.
    """
    if not chunk_files:
        raise ValueError("Nenhum chunk fornecido para unificação.")

    log.info(f"Unificando {len(chunk_files)} chunks.")

    try:
        df = spark.read.option("header", header).csv(chunk_files)

        log.info("Unificação dos chunks concluída com sucesso.")
        return df

    except FileNotFoundError as e:

        log.error(f"Arquivo não encontrado durante a leitura: {e}")
        raise

    except Exception as e:

        log.error(f"Erro ao ler os arquivos CSV com o Spark: {e}")
        raise IOError(f"Falha ao unificar chunks: {e}") from e
