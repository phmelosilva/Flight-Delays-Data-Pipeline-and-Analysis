from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_flights")


def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    """
    Verifica se o dataset possui ao menos um registro.

    Args:
        df (DataFrame): Dataset a ser validado.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se o dataset estiver vazio.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality][SilverFlights] {name}: dataset vazio.")

    log.info(f"[Quality][SilverFlights]     _check_row_count_not_empty: {name} OK.")


def _check_departure_before_arrival(df: DataFrame, name: str) -> None:
    """
    Verifica se departure_time < arrival_time, ignorando nulos.

    Args:
        df (DataFrame): Dataset com colunas de horário.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se existirem registros inválidos.
    """
    invalid = df.filter(
        F.col("departure_time").isNotNull()
        & F.col("arrival_time").isNotNull()
        & (F.col("departure_time") >= F.col("arrival_time"))
    ).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][SilverFlights] {name}: {invalid:,} voos com partida >= chegada."
        )

    log.info(f"[Quality][SilverFlights]     _check_departure_before_arrival: {name} OK.")


def _check_origin_dest_different(df: DataFrame, name: str) -> None:
    """
    Verifica se os aeroportos de origem e destino não são iguais.

    Args:
        df (DataFrame): Dataset de voos.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se existirem registros com origem == destino.
    """
    same = df.filter(
        F.col("origin_airport").isNotNull()
        & F.col("destination_airport").isNotNull()
        & (F.col("origin_airport") == F.col("destination_airport"))
    ).count()

    if same > 0:
        raise ValueError(
            f"[Quality][SilverFlights] {name}: {same:,} voos com origem == destino."
        )

    log.info(f"[Quality][SilverFlights]     _check_origin_dest_different: {name} OK.")


def _check_positive_distance(df: DataFrame, name: str) -> None:
    """
    Verifica se distance > 0, ignorando nulos.

    Args:
        df (DataFrame): Dataset contendo a coluna distance.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se houver distâncias inválidas.
    """
    invalid = df.filter(
        F.col("distance").isNotNull() & (F.col("distance") <= 0)
    ).count()

    if invalid > 0:
        raise ValueError(
            f"[Quality][SilverFlights] {name}: {invalid:,} voos com distância não positiva."
        )

    log.info(f"[Quality][SilverFlights]     _check_positive_distance: {name} OK.")


def _check_delay_consistency(df: DataFrame, name: str) -> None:
    """
    Verifica se a soma dos atrasos (motivos) é consistente com arrival_delay.

    Fórmula usada:
        reason_sum = sum(coalesce(col, 0))

    Considera chegada atrasada apenas quando arrival_delay > 0.

    Args:
        df (DataFrame): Dataset de voos.
        name (str): Nome lógico do dataset.

    Raises:
        ValueError: Se houver inconsistências maiores que 5 minutos.
    """
    delay_cols = [
        "air_system_delay",
        "security_delay",
        "airline_delay",
        "late_aircraft_delay",
        "weather_delay",
    ]

    # Monta soma dos atrasos de forma explícita
    reason_sum = None
    for col in delay_cols:
        c = F.coalesce(F.col(col), F.lit(0.0))
        reason_sum = c if reason_sum is None else (reason_sum + c)

    df_check = (
        df.filter(F.col("arrival_delay") > 0)
          .withColumn("reason_sum", reason_sum)
    )

    inconsistent = df_check.filter(
        (F.col("reason_sum") > 0)
        & (F.abs(F.col("arrival_delay") - F.col("reason_sum")) > 5.0)
    ).count()

    if inconsistent > 0:
        raise ValueError(
            f"[Quality][SilverFlights] {name}: {inconsistent:,} atrasos inconsistentes."
        )

    log.info(f"[Quality][SilverFlights]     _check_delay_consistency: {name} OK.")


def _check_referential_integrity(
    df: DataFrame,
    right_df: DataFrame,
    left_key: str,
    right_key: str,
    left_name: str,
    right_name: str,
) -> None:
    """
    Verifica se todas as chaves em left_key existem em right_key.

    Args:
        df (DataFrame): Dataset com FK.
        right_df (DataFrame): Dataset com PK.
        left_key (str): Coluna FK.
        right_key (str): Coluna PK correspondente.
        left_name (str): Nome lógico do dataset de origem.
        right_name (str): Nome lógico do dataset de referência.

    Raises:
        ValueError: Se existirem chaves sem correspondência na dimensão.
    """
    missing = (
        df.select(left_key).distinct()
          .join(
              right_df.select(right_key).distinct(),
              df[left_key] == right_df[right_key],
              "left_anti",
          )
          .count()
    )

    if missing > 0:
        raise ValueError(
            f"[Quality][SilverFlights] {left_name}: {missing:,} chaves '{left_key}' "
            f"não encontradas em {right_name}.{right_key}."
        )

    log.info(f"[Quality][SilverFlights]     _check_referential_integrity: {left_name} OK.")


def run_quality_gates_silver_flights(
    flights_df: DataFrame,
    airports_df: DataFrame,
) -> None:
    """
    Executa as validações de qualidade para a tabela flights_silver.

    Args:
        flights_df (DataFrame): Dataset de voos já refinado.
        airports_df (DataFrame): Dataset de aeroportos para integridade referencial.

    Raises:
        ValueError: Em caso de falhas nas validações.
    """
    spark = flights_df.sparkSession

    # Desativa WSCG para logs mais claros durante os checks
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    # Seleciona apenas colunas relevantes para validação
    flights_tmp = flights_df.select(
        "departure_time", "arrival_time",
        "origin_airport", "destination_airport",
        "distance", "arrival_delay",
        "air_system_delay", "security_delay",
        "airline_delay", "late_aircraft_delay", "weather_delay",
    ).cache()

    airports_tmp = airports_df.select("airport_iata_code").cache()

    flights_tmp.count()
    airports_tmp.count()

    # Quality gates
    _check_row_count_not_empty(flights_tmp, "flights_silver")
    _check_departure_before_arrival(flights_tmp, "flights_silver")
    _check_origin_dest_different(flights_tmp, "flights_silver")
    _check_positive_distance(flights_tmp, "flights_silver")
    _check_delay_consistency(flights_tmp, "flights_silver")

    _check_referential_integrity(
        flights_tmp, airports_tmp,
        left_key="origin_airport",
        right_key="airport_iata_code",
        left_name="flights_silver",
        right_name="airports_silver",
    )

    _check_referential_integrity(
        flights_tmp, airports_tmp,
        left_key="destination_airport",
        right_key="airport_iata_code",
        left_name="flights_silver",
        right_name="airports_silver",
    )

    flights_tmp.unpersist()
    airports_tmp.unpersist()

    # Restaura WSCG
    spark.conf.set("spark.sql.codegen.wholeStage", "true")
