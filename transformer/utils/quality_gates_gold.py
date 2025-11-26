from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_gold")


def _check_unique(df: DataFrame, column: str, name: str) -> None:
    """
    Verifica se uma coluna possui valores únicos no DataFrame.

    Args:
        df (DataFrame): DataFrame a ser validado.
        column (str): Nome da coluna a ser verificada quanto à unicidade.
        name (str): Nome lógico do dataset (ex.: 'dim_airline').

    Raises:
        ValueError: Se forem encontrados valores duplicados na coluna.
    """
    duplicates = (
        df.select(column)
          .groupBy(column)
          .agg(F.count("*").alias("cnt"))
          .filter(F.col("cnt") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            f"[Quality][Gold] {name}: valores duplicados em '{column}'."
        )

    log.info(f"[Quality][Gold]      _check_unique: '{column}' OK.")


def _check_no_nulls(df: DataFrame, columns: list[str], name: str) -> None:
    """
    Verifica se colunas obrigatórias não possuem valores nulos.

    Args:
        df (DataFrame): DataFrame a ser validado.
        columns (list[str]): Lista de colunas obrigatórias.
        name (str): Nome lógico do dataset (ex.: 'fato_flights').

    Raises:
        ValueError: Se qualquer coluna obrigatória possuir valores nulos.
    """
    # Cria filtro 'OR' para detectar valores nulos nas colunas obrigatórias
    null_filter = F.col(columns[0]).isNull()
    for col in columns[1:]:
        null_filter |= F.col(col).isNull()

    has_nulls = df.filter(null_filter).limit(1).count() > 0

    if has_nulls:
        problem_cols = [
            col for col in columns
            if df.filter(F.col(col).isNull()).limit(1).count() > 0
        ]
        raise ValueError(
            f"[Quality][Gold] {name}: valores nulos encontrados nas colunas: {problem_cols}."
        )

    log.info(f"[Quality][Gold]      _check_no_nulls: {name} OK.")


def _check_fk_integrity(df_fact, df_dim, fk_col, dim_col, fact_name, dim_name) -> None:
    """
    Verifica integridade referencial entre fato e dimensão.

    Args:
        df_fact (DataFrame): Dataset de fatos.
        df_dim (DataFrame): Dataset da dimensão correspondente.
        fk_col (str): Coluna FK no fato.
        dim_col (str): Coluna PK na dimensão.
        fact_name (str): Nome lógico do fato.
        dim_name (str): Nome lógico da dimensão.

    Raises:
        ValueError: Se existirem chaves no fato que não possuem correspondência na dimensão.
    """
    fact_keys = df_fact.select(F.col(fk_col).alias("fk")).distinct()
    dim_keys = df_dim.select(F.col(dim_col).alias("pk")).distinct()

    # Busca chaves ausentes na dimensão
    missing = fact_keys.join(dim_keys, fact_keys.fk == dim_keys.pk, "left_anti")

    if missing.limit(1).count() > 0:
        count_missing = missing.count()
        raise ValueError(
            f"[Quality][Gold] Integridade violada: {count_missing} "
            f"chaves de {fact_name}.{fk_col} não encontradas em {dim_name}.{dim_col}."
        )

    log.info(
        f"[Quality][Gold]           _check_fk_integrity: "
        f"[{fact_name}] '{fk_col}' <-> '{dim_name}.{dim_col}' OK."
    )


def run_quality_gates_gold(dim_airline, dim_airport, dim_date, fato_flights) -> None:
    """
    Executa todas as validações da camada Gold.

    Args:
        dim_airline (DataFrame): Dimensão de companhias aéreas.
        dim_airport (DataFrame): Dimensão de aeroportos.
        dim_date (DataFrame): Dimensão de datas.
        fato_flights (DataFrame): Tabela fato de voos.

    Raises:
        ValueError: Caso qualquer validação falhe.
    """
    log.info("[Quality][Gold] Iniciando validações.")

    # Cache para melhorar desempenho em múltiplas verificações
    dim_airline.cache()
    dim_airport.cache()
    dim_date.cache()
    fato_flights.cache()

    _check_unique(dim_airline, "airline_iata_code", "dim_airline")
    _check_unique(dim_airport, "airport_iata_code", "dim_airport")
    _check_unique(dim_date, "full_date", "dim_date")
    _check_unique(fato_flights, "flight_id", "fato_flights")

    _check_no_nulls(
        fato_flights,
        ["airline_id", "origin_airport_id", "dest_airport_id", "full_date"],
        "fato_flights",
    )

    _check_fk_integrity(
        fato_flights, dim_airline, "airline_id", "airline_id",
        "fato_flights", "dim_airline",
    )
    _check_fk_integrity(
        fato_flights, dim_airport, "origin_airport_id", "airport_id",
        "fato_flights", "dim_airport",
    )
    _check_fk_integrity(
        fato_flights, dim_airport, "dest_airport_id", "airport_id",
        "fato_flights", "dim_airport",
    )
    _check_fk_integrity(
        fato_flights, dim_date, "full_date", "full_date",
        "fato_flights", "dim_date",
    )

    log.info("[Quality][Gold] Todas as validações concluídas com sucesso.")
