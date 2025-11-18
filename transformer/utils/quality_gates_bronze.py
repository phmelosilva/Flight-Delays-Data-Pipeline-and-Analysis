from pyspark.sql import DataFrame
from typing import List
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_bronze")


# Row Count Not Empty
def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    """
    Verifica se o DataFrame não está vazio.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset (ex.: 'flights_bronze').

    Raises:
        ValueError: Se o DataFrame estiver vazio.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality][Landing] O dataset '{name}' está vazio.")

    log.info(f"[Quality][Landing]       _check_row_count_not_empty: OK")


# Schema Validation
def _check_schema_columns(df: DataFrame, required_columns: List[str], name: str) -> None:
    """
    Confirma que todas as colunas obrigatórias estão presentes no DataFrame.

    Args:
        df (DataFrame): Dataset a ser validado.
        required_columns (List[str]): Lista de colunas obrigatórias.
        name (str): Nome lógico do dataset (ex.: 'flights_bronze').

    Raises:
        ValueError: Se alguma coluna obrigatória estiver ausente.
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"[Quality][Landing] '{name}' possui colunas ausentes: {missing}.")

    log.info(f"[Quality][Landing]       _check_schema_columns: OK")


# Executor
def run_quality_gates_bronze(df: DataFrame, name: str, required_columns: List[str]) -> None:
    """
    Executa as verificações de qualidade da camada Bronze.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset (ex.: 'flights_bronze').
        required_columns (List[str]): Lista de colunas obrigatórias.

    Raises:
        ValueError: Se qualquer verificação falhar.
    """
    log.info(f"[Quality][Landing] Iniciando validações do dataset '{name}'.")

    _check_row_count_not_empty(df, name)
    _check_schema_columns(df, required_columns, name)

    log.info(f"[Quality][Landing] Todas as validações para '{name}' concluídas com sucesso.")
