from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_gold")


# Verificação de unicidade
def _check_unique(df: DataFrame, column: str, name: str) -> None:
    """
    Valida que todos os valores de uma coluna sejam únicos.

    Args:
        df (DataFrame): DataFrame a ser verificado.
        column (str): Nome da coluna cuja unicidade será testada.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se existirem valores duplicados na coluna especificada.
    """
    duplicates = df.groupBy(column).count().filter(F.col("count") > 1)
    if duplicates.count() > 0:
        raise ValueError(f"[Quality] {name}: valores duplicados em '{column}'.")

    log.info(f"[Quality] {name}: unicidade de '{column}' OK.")


# Verificação de valores nulos
def _check_no_nulls(df: DataFrame, columns: list[str], name: str) -> None:
    """
    Valida que nenhuma das colunas especificadas contenha valores nulos.

    Args:
        df (DataFrame): DataFrame a ser verificado.
        columns (list[str]): Lista de colunas críticas que não devem conter nulos.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se qualquer coluna crítica contiver valores nulos.
    """
    for c in columns:
        nulls = df.filter(F.col(c).isNull())
        if nulls.count() > 0:
            raise ValueError(f"[Quality] {name}: valores nulos encontrados em '{c}'.")

    log.info(f"[Quality] {name}: sem valores nulos nas colunas críticas.")

# Verificação de integridade referencial
def _check_fk_integrity(
    df_fact: DataFrame,
    df_dim: DataFrame,
    fk_col: str,
    dim_col: str,
    fact_name: str,
    dim_name: str
) -> None:
    """
    Valida a integridade referencial entre um DataFrame de fatos e uma dimensão.

    Args:
        df_fact (DataFrame): DataFrame da tabela fato.
        df_dim (DataFrame): DataFrame da tabela dimensão.
        fk_col (str): Coluna de chave estrangeira no fato.
        dim_col (str): Coluna de chave primária na dimensão.
        fact_name (str): Nome lógico do fato (para logs).
        dim_name (str): Nome lógico da dimensão (para logs).

    Raises:
        ValueError: Se existirem chaves estrangeiras sem correspondência na dimensão.
    """
    fact_alias = df_fact.select(F.col(fk_col).alias("fact_fk")).distinct()
    dim_alias = df_dim.select(F.col(dim_col).alias("dim_pk")).distinct()

    missing = (
        fact_alias
        .join(dim_alias, on=F.col("fact_fk") == F.col("dim_pk"), how="left_anti")
    )

    missing_count = missing.count()
    if missing_count > 0:
        raise ValueError(
            f"[Quality] Integridade violada: {missing_count} chaves de "
            f"{fact_name}.{fk_col} não encontradas em {dim_name}.{dim_col}."
        )

    log.info(f"[Quality] {fact_name}: integridade de fk '{fk_col}' -> {dim_name}.{dim_col} - OK.")


# Executor principal
def run_quality_gates_gold(
    dim_airline: DataFrame,
    dim_airport: DataFrame,
    dim_date: DataFrame,
    fato_flights: DataFrame,
) -> None:
    """
    Executa o conjunto de verificações de qualidade da camada gold.

    Args:
        dim_airline (DataFrame): Dataset da dimensão de companhias aéreas.
        dim_airport (DataFrame): Dataset da dimensão de aeroportos.
        dim_date (DataFrame): Dataset da dimensão de datas.
        fato_flights (DataFrame): Dataset da tabela fato de voos.

    Raises:
        ValueError: Se qualquer verificação de qualidade falhar.
    """
    log.info("[Quality] Iniciando validações...")

    # Unicidade das PKs e naturais
    _check_unique(dim_airline, "airline_iata_code", "dim_airline")
    _check_unique(dim_airport, "airport_iata_code", "dim_airport")
    _check_unique(dim_date, "full_date", "dim_date")
    _check_unique(fato_flights, "flight_id", "fato_flights")

    # Ausência de nulos nas FKs
    fk_cols = ["airline_id", "origin_airport_id", "dest_airport_id", "full_date"]
    _check_no_nulls(fato_flights, fk_cols, "fato_flights")

    # Integridade referencial fato -> dimensões
    _check_fk_integrity(
        fato_flights,
        dim_airline,
        "airline_id",
        "airline_id",
        "fato_flights",
        "dim_airline"
    )
    _check_fk_integrity(
        fato_flights,
        dim_airport,
        "origin_airport_id",
        "airport_id",
        "fato_flights",
        "dim_airport"
    )
    _check_fk_integrity(
        fato_flights,
        dim_airport,
        "dest_airport_id",
        "airport_id",
        "fato_flights",
        "dim_airport"
    )
    _check_fk_integrity(
        fato_flights,
        dim_date,
        "full_date",
        "full_date",
        "fato_flights",
        "dim_date"
    )

    log.info("[Quality] Todas as validações concluídas com sucesso.")
