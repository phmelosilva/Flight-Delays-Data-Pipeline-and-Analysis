from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver_flights")


# Verificação de dataset não vazio
def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality] {name}: dataset vazio.")
    log.info(f"[Quality]                _check_row_count_not_empty: OK")


# Partida antes da chegada
def _check_departure_before_arrival(df: DataFrame, name: str) -> None:
    invalid = df.filter(
        F.col("departure_time").isNotNull()
        & F.col("arrival_time").isNotNull()
        & (F.col("departure_time") >= F.col("arrival_time"))
    ).count()
    if invalid > 0:
        raise ValueError(f"[Quality] {name}: {invalid:,} voos com partida >= chegada.")

    log.info(f"[Quality]                _check_departure_before_arrival: OK")


# Origem e destino diferentes
def _check_origin_dest_different(df: DataFrame, name: str) -> None:
    same = df.filter(
        F.col("origin_airport").isNotNull()
        & F.col("destination_airport").isNotNull()
        & (F.col("origin_airport") == F.col("destination_airport"))
    ).count()
    if same > 0:
        raise ValueError(f"[Quality] {name}: {same:,} voos com origem == destino.")

    log.info(f"[Quality]                _check_origin_dest_different: OK")


# Distância positiva
def _check_positive_distance(df: DataFrame, name: str) -> None:
    invalid = df.filter(F.col("distance").isNotNull() & (F.col("distance") <= 0)).count()
    if invalid > 0:
        raise ValueError(f"[Quality] {name}: {invalid:,} voos com distância não positiva.")

    log.info(f"[Quality]                _check_positive_distance: OK")


# Consistência dos atrasos
def _check_delay_consistency(df: DataFrame, name: str) -> None:
    delay_cols = [
        "air_system_delay",
        "security_delay",
        "airline_delay",
        "late_aircraft_delay",
        "weather_delay",
    ]

    df_check = df.filter(F.col("arrival_delay") > 0).withColumn(
        "reason_sum", sum(F.coalesce(F.col(c), F.lit(0.0)) for c in delay_cols)
    )

    inconsistent = df_check.filter(
        (F.col("reason_sum") > 0)
        & (F.abs(F.col("arrival_delay") - F.col("reason_sum")) > 5.0)
    ).count()

    if inconsistent > 0:
        raise ValueError(f"[Quality] {name}: {inconsistent:,} atrasos inconsistentes.")

    log.info(f"[Quality]                _check_delay_consistency: OK")


# Integridade referencial
def _check_referential_integrity(
    df: DataFrame,
    right_df: DataFrame,
    left_key: str,
    right_key: str,
    left_name: str,
    right_name: str,
) -> None:
    missing = (
        df.select(left_key).distinct()
        .join(right_df.select(right_key).distinct(),
              df[left_key] == right_df[right_key],
              "left_anti")
        .count()
    )

    if missing > 0:
        raise ValueError(
            f"[Quality] {left_name}: {missing:,} chaves '{left_key}' "
            f"não encontradas em {right_name}.{right_key}."
        )

    log.info(f"[Quality]                _check_referential_integrity: OK")


# Executor
def run_quality_gates_silver_flights(
    flights_df: DataFrame,
    airports_df: DataFrame,
) -> None:
    spark = flights_df.sparkSession
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    # Seleciona apenas as colunas necessárias para quality
    flights_tmp = flights_df.select(
        "departure_time","arrival_time",
        "origin_airport","destination_airport",
        "distance","arrival_delay",
        "air_system_delay","security_delay",
        "airline_delay","late_aircraft_delay","weather_delay"
    ).cache()

    airports_tmp = airports_df.select("airport_iata_code").cache()

    # Materializa
    flights_tmp.count()
    airports_tmp.count()

    # Quality gates
    _check_row_count_not_empty(flights_tmp, "flights_silver")
    _check_departure_before_arrival(flights_tmp, "flights_silver")
    _check_origin_dest_different(flights_tmp, "flights_silver")
    _check_positive_distance(flights_tmp, "flights_silver")
    _check_delay_consistency(flights_tmp, "flights_silver")

    # Integridade referencial: origem
    _check_referential_integrity(
        flights_tmp,
        airports_tmp,
        left_key="origin_airport",
        right_key="airport_iata_code",
        left_name="flights_silver",
        right_name="airports_silver",
    )

    # Integridade referencial: destino
    _check_referential_integrity(
        flights_tmp,
        airports_tmp,
        left_key="destination_airport",
        right_key="airport_iata_code",
        left_name="flights_silver",
        right_name="airports_silver",
    )

    flights_tmp.unpersist()
    airports_tmp.unpersist()

    spark.conf.set("spark.sql.codegen.wholeStage", "true")
