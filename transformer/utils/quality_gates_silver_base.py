from pyspark.sql import DataFrame, functions as F
from typing import List
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_base")


def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    """
    Valida que o DataFrame contém pelo menos um registro.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality][Refinement] {name}: dataset vazio.")

    log.info(f"[Quality][Refinement]    _check_row_count_not_empty: OK")

def _check_schema_columns(df: DataFrame, required_columns: List[str], name: str) -> None:
    """
    Confirma que todas as colunas obrigatórias estão presentes no DataFrame.
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"[Quality][Refinement] {name}: colunas ausentes {missing}.")

    log.info(f"[Quality][Refinement]    _check_schema_columns: OK")

def _check_no_null_primary_key(df: DataFrame, pk_columns: List[str], name: str) -> None:
    """
    Garante que as colunas de chave primária não possuam valores nulos.
    """
    condition = " OR ".join([f"{col} IS NULL" for col in pk_columns])
    invalid = df.filter(condition).count()
    if invalid > 0:
        raise ValueError(f"[Quality][Refinement] {name}: {invalid:,} registros com PK nula.")

    log.info(f"[Quality][Refinement]    _check_no_null_primary_key: OK")

def _check_unique_primary_key(df: DataFrame, pk_columns: List[str], name: str) -> None:
    """
    Valida que a chave primária é única dentro do dataset.
    """
    dup_count = (
        df.groupBy([F.col(c) for c in pk_columns])
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_count > 0:
        raise ValueError(f"[Quality][Refinement] {name}: {dup_count:,} duplicatas encontradas na pk.")

    log.info(f"[Quality][Refinement]    _check_unique_primary_key: OK")

def _check_no_full_duplicates(df: DataFrame, name: str) -> None:
    """
    Verifica se não existem registros completamente duplicados.
    """
    dup_rows = df.groupBy(df.columns).count().filter(F.col("count") > 1).count()
    if dup_rows > 0:
        raise ValueError(f"[Quality][Refinement] {name}: {dup_rows:,} linhas duplicadas completas.")

    log.info(f"[Quality][Refinement]    _check_no_full_duplicates: OK")


def run_quality_gates_silver_base(
    df: DataFrame,
    name: str,
    required_columns: List[str],
    pk_columns: List[str],
) -> None:
    """
    Executa as verificações de qualidade para datasets base da Silver.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset (ex.: 'airlines_silver').
        required_columns (List[str]): Lista de colunas obrigatórias.
        pk_columns (List[str]): Lista de colunas consideradas pk.

    Raises:
        ValueError: Se qualquer verificação falhar.
    """
    log.info(f"[Quality][Refinement] Iniciando validações do dataset '{name}'.")

    _check_row_count_not_empty(df, name)
    _check_schema_columns(df, required_columns, name)
    _check_no_null_primary_key(df, pk_columns, name)
    _check_unique_primary_key(df, pk_columns, name)
    _check_no_full_duplicates(df, name)

    log.info(f"[Quality][Refinement] Validações para '{name}' concluídas.")
