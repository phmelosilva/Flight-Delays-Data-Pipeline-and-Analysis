from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_aggregated")


def _check_row_count_not_empty(df: DataFrame) -> None:
    """
    Verifica se o DataFrame não está vazio.

    Args:
        df (DataFrame): Dataset a ser validado.

    Raises:
        ValueError: Se o DataFrame não possuir registros.
    """
    if df.limit(1).count() == 0:
        raise ValueError("[Quality][Aggregate] Dataset agregado vazio.")

    log.info("[Quality][Aggregate]      _check_row_count_not_empty: OK.")


def _check_unique_primary_key(df: DataFrame, key: str = "flight_id") -> None:
    """
    Verifica unicidade da chave primária do dataset agregado.

    Args:
        df (DataFrame): Dataset a ser validado.
        key (str): Nome da coluna que representa a PK.

    Raises:
        ValueError: Se houver chaves duplicadas.
    """
    dups = (
        df.groupBy(F.col(key))
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    if dups > 0:
        raise ValueError(
            f"[Quality][Aggregate] primary key duplicada em {dups:,} registros "
            f"(coluna '{key}')."
        )

    log.info("[Quality][Aggregate]      _check_unique_primary_key: OK.")


def _check_no_null_in_dimensions(df: DataFrame) -> None:
    """
    Verifica se dimensões críticas não possuem valores nulos.

    As colunas obrigatórias são:
        - airline_iata_code
        - airline_name
        - origin_airport_iata_code
        - origin_airport_name
        - origin_latitude
        - origin_longitude
        - dest_airport_iata_code
        - dest_airport_name
        - dest_latitude
        - dest_longitude

    Args:
        df (DataFrame): Dataset a ser validado.

    Raises:
        KeyError: Se uma coluna crítica estiver ausente.
        ValueError: Se houver valores nulos em colunas críticas.
    """
    critical_cols = [
        "airline_iata_code",
        "airline_name",
        "origin_airport_iata_code",
        "origin_airport_name",
        "origin_latitude",
        "origin_longitude",
        "dest_airport_iata_code",
        "dest_airport_name",
        "dest_latitude",
        "dest_longitude",
    ]

    cols_with_nulls = []

    for col in critical_cols:
        if col not in df.columns:
            raise KeyError(
                f"[Quality][Aggregate] Coluna crítica ausente: '{col}'."
            )

        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            cols_with_nulls.append((col, nulls))

    if cols_with_nulls:
        details = ", ".join([f"{c}={n:,}" for c, n in cols_with_nulls])
        raise ValueError(
            f"[Quality][Aggregate] Nulos encontrados em dimensões críticas: {details}."
        )

    log.info("[Quality][Aggregate]      _check_no_null_in_dimensions: OK.")


def _check_positive_distance(df: DataFrame) -> None:
    """
    Verifica se todos os valores de distância são positivos.
    Registros com distância nula são ignorados.

    Args:
        df (DataFrame): Dataset contendo a coluna 'distance'.

    Raises:
        ValueError: Se existirem distâncias menores ou iguais a zero.
    """
    if "distance" not in df.columns:
        log.info(
            "[Quality][Aggregate] _check_positive_distance: coluna 'distance' ausente; ignorando verificação."
        )
        return

    invalid = df.filter(
        F.col("distance").isNotNull() & (F.col("distance") <= 0)
    ).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][Aggregate] {invalid:,} registros com distância não positiva."
        )

    log.info("[Quality][Aggregate]      _check_positive_distance: OK.")


def _check_departure_before_arrival(df: DataFrame) -> None:
    """
    Verifica se departure_time < arrival_time.
    Registros nulos são ignorados.

    Args:
        df (DataFrame): Dataset contendo 'departure_time' e 'arrival_time'.

    Raises:
        ValueError: Se houver registros inválidos.
    """
    for col in ["departure_time", "arrival_time"]:
        if col not in df.columns:
            log.info(
                "[Quality][Aggregate] _check_departure_before_arrival: colunas incompletas; ignorando verificação."
            )
            return

    invalid = df.filter(
        F.col("departure_time").isNotNull()
        & F.col("arrival_time").isNotNull()
        & (F.col("departure_time") >= F.col("arrival_time"))
    ).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][Aggregate] {invalid:,} voos com departure_time >= arrival_time."
        )

    log.info("[Quality][Aggregate]      _check_departure_before_arrival: OK.")


def _check_origin_dest_different(df: DataFrame) -> None:
    """
    Verifica se origem e destino não são iguais.

    Args:
        df (DataFrame): Dataset contendo códigos IATA de origem e destino.

    Raises:
        ValueError: Se houver registros com origem == destino.
    """
    for col in ["origin_airport_iata_code", "dest_airport_iata_code"]:
        if col not in df.columns:
            log.info(
                f"[Quality][Aggregate] {col} incompleta, ignorando verificação."
            )
            return

    same = df.filter(
        F.col("origin_airport_iata_code").isNotNull()
        & F.col("dest_airport_iata_code").isNotNull()
        & (
            F.col("origin_airport_iata_code")
            == F.col("dest_airport_iata_code")
        )
    ).count()

    if same > 0:
        raise ValueError(
            f"[Quality][Aggregate] {same:,} voos com origem igual ao destino."
        )

    log.info("[Quality][Aggregate]      _check_origin_dest_different: OK.")


def run_quality_gates_silver_aggregated(df: DataFrame) -> None:
    """
    Executa todas as validações de qualidade para o dataset agregado da Silver.

    Args:
        df (DataFrame): Dataset agregado (flights + airlines + airports).

    Raises:
        ValueError: Para qualquer problema identificado nas validações.
        KeyError: Se colunas obrigatórias estiverem ausentes.
    """
    log.info("[Quality][Aggregate] Iniciando validações do dataset agregado.")

    _check_row_count_not_empty(df)
    _check_unique_primary_key(df, key="flight_id")
    _check_no_null_in_dimensions(df)
    _check_positive_distance(df)
    _check_departure_before_arrival(df)
    _check_origin_dest_different(df)

    log.info("[Quality][Aggregate] Validações concluídas com sucesso.")
