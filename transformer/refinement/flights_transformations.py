from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from airflow.utils.log.logging_mixin import LoggingMixin
from transformer.utils.helpers import to_date_from_ymd


log = LoggingMixin().log

def transform_flights(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações iniciais na tabela de voos, antes do join.
    """
    base_required = {"YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER"}

    missing = base_required - set(df.columns)
    if missing:
        raise KeyError(f"Colunas obrigatórias faltando em flights source: {missing}")

    df2 = df.filter(
        (F.col("DIVERTED").isNull() | (F.col("DIVERTED") != 1)) &
        (F.col("CANCELLED").isNull() | (F.col("CANCELLED") != 1))
    )

    df2 = (
        df2.withColumn("flight_date", to_date_from_ymd(F.col("YEAR"), F.col("MONTH"), F.col("DAY")))
        .withColumnRenamed("YEAR", "flight_year")
        .withColumnRenamed("MONTH", "flight_month")
        .withColumnRenamed("DAY", "flight_day")
        .withColumnRenamed("DAY_OF_WEEK", "flight_day_of_week")
    )

    numeric_cols = [
        "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME",
        "WHEELS_OFF", "WHEELS_ON", "DEPARTURE_DELAY", "ARRIVAL_DELAY", "TAXI_OUT",
        "TAXI_IN", "AIR_TIME", "ELAPSED_TIME", "SCHEDULED_TIME", "DISTANCE",
        "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY",
        "WEATHER_DELAY"
    ]
    for c in numeric_cols:
        if c in df2.columns:
            df2 = df2.withColumn(c, F.col(c).cast(DoubleType()))

    delay_cols = ["AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY",
                  "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]
    for c in delay_cols:
        if c in df2.columns:
            df2 = df2.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    drop_cols = ["DIVERTED", "CANCELLED", "CANCELLATION_REASON"]
    df2 = df2.drop(*drop_cols)

    log.info("Transformação FLIGHTS concluída.")
    return df2


def create_aggregated_flights_df(flights_silver_df: DataFrame, airlines_silver_df: DataFrame, airports_silver_df: DataFrame) -> DataFrame:
    """
    Agrega os DataFrames já transformados da camada Silver em uma única tabela denormalizada.

    Args:
        flights_silver_df (DataFrame): DataFrame de voos já processado.
        airlines_silver_df (DataFrame): DataFrame de companhias aéreas já processado.
        airports_silver_df (DataFrame): DataFrame de aeroportos já processado.

    Returns:
        DataFrame: O DataFrame final agregado, pronto para os quality gates e carga.
    """
    aggregated = flights_silver_df.join(
        airlines_silver_df,
        flights_silver_df.AIRLINE == airlines_silver_df.airline_iata_code,
        "left"
    )

    aggregated = aggregated.join(
        airports_silver_df.alias("origin_airport"),
        aggregated.ORIGIN_AIRPORT == F.col("origin_airport.airport_iata_code"),
        "left"
    )

    aggregated = aggregated.join(
        airports_silver_df.alias("dest_airport"),
        aggregated.DESTINATION_AIRPORT == F.col("dest_airport.airport_iata_code"),
        "left"
    )

    final_df = aggregated.select(
        F.monotonically_increasing_id().alias("flight_id"),
        F.col("flight_year"),
        F.col("flight_month"),
        F.col("flight_day"),
        F.col("flight_day_of_week"),
        F.col("flight_date"),
        F.col("airline_iata_code"),
        F.col("airline_name"),
        F.col("FLIGHT_NUMBER").alias("flight_number"),
        F.col("TAIL_NUMBER").alias("tail_number"),
        F.col("origin_airport.airport_iata_code").alias("origin_airport_iata_code"),
        F.col("origin_airport.AIRPORT").alias("origin_airport_name"),
        F.col("origin_airport.CITY").alias("origin_city"),
        F.col("origin_airport.STATE").alias("origin_state"),
        F.col("origin_airport.LATITUDE").alias("origin_latitude"),
        F.col("origin_airport.LONGITUDE").alias("origin_longitude"),
        F.col("dest_airport.airport_iata_code").alias("dest_airport_iata_code"),
        F.col("dest_airport.AIRPORT").alias("dest_airport_name"),
        F.col("dest_airport.CITY").alias("dest_city"),
        F.col("dest_airport.STATE").alias("dest_state"),
        F.col("dest_airport.LATITUDE").alias("dest_latitude"),
        F.col("dest_airport.LONGITUDE").alias("dest_longitude"),
        F.col("SCHEDULED_DEPARTURE").alias("scheduled_departure"),
        F.col("DEPARTURE_TIME").alias("departure_time"),
        F.col("SCHEDULED_ARRIVAL").alias("scheduled_arrival"),
        F.col("ARRIVAL_TIME").alias("arrival_time"),
        F.col("WHEELS_OFF").alias("wheels_off"),
        F.col("WHEELS_ON").alias("wheels_on"),
        F.col("DEPARTURE_DELAY").alias("departure_delay"),
        F.col("ARRIVAL_DELAY").alias("arrival_delay"),
        F.col("TAXI_OUT").alias("taxi_out"),
        F.col("TAXI_IN").alias("taxi_in"),
        F.col("AIR_TIME").alias("air_time"),
        F.col("ELAPSED_TIME").alias("elapsed_time"),
        F.col("SCHEDULED_TIME").alias("scheduled_time"),
        F.col("DISTANCE").alias("distance"),
        F.col("AIR_SYSTEM_DELAY").alias("air_system_delay"),
        F.col("SECURITY_DELAY").alias("security_delay"),
        F.col("AIRLINE_DELAY").alias("airline_delay"),
        F.col("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay"),
        F.col("WEATHER_DELAY").alias("weather_delay")
    )

    return final_df
