from typing import List
from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_base")


def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    """
    Verifica se o DataFrame não está vazio.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se o DataFrame estiver vazio.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality][Silver] {name}: dataset vazio.")

    log.info(f"[Quality][Silver]    _check_row_count_not_empty: {name} OK.")


def _check_schema_columns(df: DataFrame, required_columns: List[str], name: str) -> None:
    """
    Verifica se todas as colunas obrigatórias existem no dataset.

    Args:
        df (DataFrame): Dataset a ser validado.
        required_columns (List[str]): Lista de colunas obrigatórias.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se houver colunas obrigatórias ausentes.
    """
    missing = set(required_columns) - set(df.columns)

    if missing:
        raise ValueError(
            f"[Quality][Silver] {name}: colunas obrigatórias ausentes: {sorted(missing)}."
        )

    log.info(f"[Quality][Silver]    _check_schema_columns: {name} OK.")


def _check_no_null_primary_key(df: DataFrame, pk_columns: List[str], name: str) -> None:
    """
    Garante que as colunas de chave primária não possuem valores nulos.

    Args:
        df (DataFrame): Dataset a ser validado.
        pk_columns (List[str]): Colunas que compõem a PK.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se algum campo de PK estiver nulo.
    """
    # Cria condição OR para detectar qualquer PK nula
    condition = " OR ".join([f"{col} IS NULL" for col in pk_columns])

    invalid = df.filter(condition).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][Silver] {name}: {invalid:,} registros com PK nula."
        )

    log.info(f"[Quality][Silver]    _check_no_null_primary_key: {name} OK.")


def _check_unique_primary_key(df: DataFrame, pk_columns: List[str], name: str) -> None:
    """
    Verifica se a chave primária é única no dataset.

    Args:
        df (DataFrame): Dataset a ser validado.
        pk_columns (List[str]): Colunas que compõem a PK.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se existirem duplicatas da PK.
    """
    dup_count = (
        df.groupBy([F.col(c) for c in pk_columns])
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    if dup_count > 0:
        raise ValueError(
            f"[Quality][Silver] {name}: {dup_count:,} duplicatas detectadas na PK."
        )

    log.info(f"[Quality][Silver]    _check_unique_primary_key: {name} OK.")


def _check_no_full_duplicates(df: DataFrame, name: str) -> None:
    """
    Verifica se não existem linhas completamente duplicadas.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se houver registros idênticos em todas as colunas.
    """
    dup_rows = (
        df.groupBy(df.columns)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    if dup_rows > 0:
        raise ValueError(
            f"[Quality][Silver] {name}: {dup_rows:,} registros duplicados completos."
        )

    log.info(f"[Quality][Silver]    _check_no_full_duplicates: {name} OK.")


def run_quality_gates_silver_base(
    df: DataFrame,
    name: str,
    required_columns: List[str],
    pk_columns: List[str],
) -> None:
    """
    Executa todas as validações de qualidade da camada Silver (base).

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset.
        required_columns (List[str]): Colunas obrigatórias.
        pk_columns (List[str]): Colunas que compõem a chave primária.

    Raises:
        ValueError: Se qualquer etapa de validação falhar.
    """
    log.info(f"[Quality][Silver] Iniciando validações do dataset '{name}'.")

    _check_row_count_not_empty(df, name)
    _check_schema_columns(df, required_columns, name)
    _check_no_null_primary_key(df, pk_columns, name)
    _check_unique_primary_key(df, pk_columns, name)
    _check_no_full_duplicates(df, name)

    log.info(f"[Quality][Silver] Todas as validações para '{name}' concluídas com sucesso.")
