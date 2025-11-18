from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_aggregated")


def _check_row_count_not_empty(df: DataFrame) -> None:
    """
    Verifica se o DataFrame agregado não está vazio.

    Args:
        df (DataFrame): DataFrame agregado a ser validado.

    Raises:
        ValueError: Se o DataFrame estiver vazio.
    """
    if df.limit(1).count() == 0:
        raise ValueError("[Quality][Aggregate] _check_row_count_not_empty: Fail")

    log.info("[Quality][Aggregate]      _check_row_count_not_empty: OK")

def _check_unique_primary_key(df: DataFrame, key: str = "flight_id") -> None:
    """
    Garante unicidade da chave primária do dataset agregado.

    Args:
        df: DataFrame a ser validado.
        key: Nome da coluna que representa a PK.

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
            f"[Quality][Aggregate] _check_unique_primary_key: FAIL. PK duplicada em {dups:,} linhas (chave='{key}')."
        )

    log.info(f"[Quality][Aggregate]     _check_unique_primary_key: OK")

def _check_no_null_in_dimensions(df: DataFrame) -> None:
    """
    Verifica se dimensões críticas não possuem valores nulos.

    As colunas abaixo são consideradas obrigatórias após a agregação:
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
        df: DataFrame agregado.

    Raises:
        ValueError: Se qualquer coluna crítica possuir nulos.
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
                f"[Quality][Aggregate] _check_no_null_in_dimensions: FAIL. Coluna crítica '{col}' ausente do dataset agregado."
            )
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            cols_with_nulls.append((col, nulls))

    if cols_with_nulls:
        details = ", ".join([f"{c}={n:,}" for c, n in cols_with_nulls])
        raise ValueError(
            f"[Quality][Aggregate] _check_no_null_in_dimensions: FAIL. Dimensões críticas com nulos: {details}."
        )

    log.info("[Quality][Aggregate]      _check_no_null_in_dimensions: OK")

def _check_positive_distance(df: DataFrame) -> None:
    """
    Verifica se todos os valores de distância são positivos.

    Registros sem valor informado são ignorados.

    Args:
        df: DataFrame agregado contendo a coluna 'distance'.

    Raises:
        ValueError: Se existirem distâncias menores ou iguais a zero.
    """
    if "distance" not in df.columns:
        log.info(
            "[Quality][Aggregate] _check_positive_distance: FAIL. Coluna 'distance' ausente"
        )
        return

    invalid = df.filter(
        F.col("distance").isNotNull() & (F.col("distance") <= 0)
    ).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][Aggregate] _check_positive_distance: FAIL. {invalid:,} registros com distância não positiva."
        )

    log.info("[Quality][Aggregate]      _check_positive_distance: OK")

def _check_departure_before_arrival(df: DataFrame) -> None:
    """
    Verifica se o horário de partida ocorre antes do horário de chegada.

    Registros com horários nulos são ignorados.

    Args:
        df: DataFrame agregado contendo as colunas 'departure_time' e 'arrival_time'.

    Raises:
        ValueError: Se houver registros com partida posterior ou igual à chegada.
    """
    for col in ["departure_time", "arrival_time"]:
        if col not in df.columns:
            log.info("[Quality][Aggregate] _check_departure_before_arrival: FAIL. Colunas de horário incompletas")
            return

    invalid = df.filter(
        F.col("departure_time").isNotNull()
        & F.col("arrival_time").isNotNull()
        & (F.col("departure_time") >= F.col("arrival_time"))
    ).count()

    if invalid > 0:
        raise ValueError(f"[Quality][Aggregate] _check_departure_before_arrival: FAIL. {invalid:,} voos com departure_time >= arrival_time.")

    log.info("[Quality][Aggregate]      _check_departure_before_arrival: OK")

def _check_origin_dest_different(df: DataFrame) -> None:
    """
    Verifica se o aeroporto de origem e destino não são iguais.

    Usa as colunas:
        - origin_airport_iata_code
        - dest_airport_iata_code

    Args:
        df: DataFrame agregado.

    Raises:
        ValueError: Se existirem registros com origem e destino idênticos.
    """
    for col in ["origin_airport_iata_code", "dest_airport_iata_code"]:
        if col not in df.columns:
            log.info(
                "[Quality][Aggregate] Colunas de origem/destino incompletas; "
                "pulando check origin_dest_different."
            )
            return

    same = df.filter(
        F.col("origin_airport_iata_code").isNotNull()
        & F.col("dest_airport_iata_code").isNotNull()
        & (F.col("origin_airport_iata_code") == F.col("dest_airport_iata_code"))
    ).count()

    if same > 0:
        raise ValueError(
            f"[Quality][Aggregate] {same:,} voos com origem == destino "
            "(origin_airport_iata_code == dest_airport_iata_code)."
        )

    log.info("[Quality][Aggregate]      _check_origin_dest_different: OK")

def run_quality_gates_silver_aggregated(df: DataFrame) -> None:
    """
    Executa o conjunto de verificações de qualidade para o dataset agregado da Silver.

    Checks executados:
        - row_count_not_empty
        - unique_primary_key (flight_id)
        - no_null_in_dimensions (dimensões críticas)
        - positive_distance
        - departure_before_arrival
        - origin_dest_different

    Args:
        df: DataFrame agregado resultante da união de flights, airlines e airports.

    Raises:
        ValueError: Se qualquer verificação de qualidade falhar.
    """
    log.info("[Quality][Aggregate] Iniciando validações do dataset agregado.")

    _check_row_count_not_empty(df)
    _check_unique_primary_key(df, key="flight_id")
    _check_no_null_in_dimensions(df)
    _check_positive_distance(df)
    _check_departure_before_arrival(df)
    _check_origin_dest_different(df)

    log.info("[Quality][Aggregate] Validações concluídas.")
