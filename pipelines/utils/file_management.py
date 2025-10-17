from pathlib import Path as PythonPath
from collections import defaultdict
from pyspark.sql import SparkSession
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def move_files(spark: SparkSession, source_files: list[str], base_dest_path: str, processing_date: str) -> None:
    """
    Move arquivos de uma área para outra, organizando-os por extensão de arquivo.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        source_files (list[str]): Lista de caminhos completos dos arquivos de origem.
        base_dest_path (str): Caminho base do diretório de destino.
        processing_date (str): A data de processamento para criar a partição.

    Raises:
        ValueError: Se a lista de arquivos estiver vazia.
        IOError: Se ocorrer erro ao criar diretórios ou mover arquivos.
    """
    if not source_files:
        raise ValueError("Nenhum caminho de arquivo fornecido.")

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    # Agrupando os arquivos por tipo.
    destinations = defaultdict(list)
    for file_path in source_files:
        py_path = PythonPath(file_path)
        extension = py_path.suffix.lstrip(".").upper() if py_path.suffix else "UNKNOWN"

        if extension == "UNKNOWN" and py_path.is_dir():
            extension = "PARQUET"

        dest_dir = f"{base_dest_path}/{processing_date}/{extension}"
        destinations[dest_dir].append(file_path)

    # Criando os diretórios de destino.
    for dest_dir_str in destinations.keys():
        dest_path = Path(dest_dir_str)
        try:
            if not fs.exists(dest_path):
                if not fs.mkdirs(dest_path):
                    raise IOError(f"Não foi possível criar diretório: {dest_dir_str}")
        except Exception as e:
            raise IOError(f"Erro ao criar diretório '{dest_dir_str}': {e}") from e

    # Movendo os arquivos.
    log.info(f"Movendo arquivos para: {base_dest_path}")
    for dest_dir_str, files_to_move in destinations.items():
        dest_path = Path(dest_dir_str)
        for file_path in files_to_move:
            source_path = Path(file_path)
            target_path = Path(f"{dest_dir_str}/{PythonPath(file_path).name}")
            try:
                if fs.rename(source_path, target_path):
                    log.info(f"Arquivo '{source_path.getName()}' movido para '{target_path}'.")
                else:
                    raise IOError(f"Falha ao mover '{file_path}' para '{target_path}'.")
            except Exception as e:
                raise IOError(f"Erro ao mover '{file_path}': {e}") from e

def delete_files(spark: SparkSession, files_to_delete: list[str]) -> None:
    """
    Deleta arquivos de um determinado local.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        files_to_delete (list[str]): Lista de caminhos dos arquivos a serem deletados.

    Raises:
        IOError: Se algum arquivo não puder deletado.
        Exception: Se ocorrer qualquer falha inesperada na operação com o sistema de arquivos.
    """
    if not files_to_delete:
        log.info("Nenhum arquivo para deletar.")
        return

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    for file_path in files_to_delete:
        path_to_delete = Path(file_path)

        try:
            if fs.exists(path_to_delete):
                if fs.delete(path_to_delete, True):
                    log.info(f"Arquivo '{file_path}' deletado com sucesso.")
                else:
                    raise IOError(f"Falha ao deletar arquivo '{file_path}'.")
            else:
                log.warning(f"Arquivo '{file_path}' não encontrado. Ignorando.")
        except Exception as e:
            log.error(f"Erro ao deletar '{file_path}': {e}")
            raise

    log.info("Limpeza concluída.")
