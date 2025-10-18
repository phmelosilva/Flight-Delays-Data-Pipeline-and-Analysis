from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

_CHECKS = {}

def quality_check(name):
    """Decorador para registrar uma função de quality check."""
    def decorator(func):
        _CHECKS[name] = func
        return func
    return decorator

@quality_check("row_count_not_empty")
def _check_row_count(df: DataFrame, **kwargs):
    if df.rdd.isEmpty():
        raise ValueError("Quality Gate Falhou: DataFrame está vazio.")

@quality_check("elapsed_time")
def _check_elapsed_time(df: DataFrame, **kwargs):
    df_check = df.withColumn("TIME_DIFFERENCE", F.col("ELAPSED_TIME") - (F.col("TAXI_OUT") + F.col("AIR_TIME") + F.col("TAXI_IN")))
    inconsistent_count = df_check.filter(F.col("TIME_DIFFERENCE") != 0).count()
    if inconsistent_count > 0:
        raise ValueError(f"Quality Gate Falhou: {inconsistent_count} voos com ELAPSED_TIME inconsistente.")

@quality_check("departure_before_arrival")
def _check_departure_before_arrival(df: DataFrame, **kwargs):
    invalid_order_count = df.filter(F.col("DEPARTURE_TIME") >= F.col("ARRIVAL_TIME")).count()
    if invalid_order_count > 0:
        raise ValueError(f"Quality Gate Falhou: {invalid_order_count} voos com chegada no mesmo minuto ou antes da partida.")

@quality_check("referential_integrity")
def _check_referential_integrity(df: DataFrame, **kwargs):
    right_df, left_key, right_key = kwargs['right_df'], kwargs['left_key'], kwargs['right_key']

    missing_keys_count = df.select(left_key).join(right_df.select(right_key), df[left_key] == right_df[right_key], "left_anti").count()
    if missing_keys_count > 0:
        raise ValueError(f"Quality Gate Falhou: {missing_keys_count} chaves de '{left_key}' não encontradas em '{right_key}'.")

@quality_check("origin_dest_different")
def _check_origin_dest_different(df: DataFrame, **kwargs):

    same_airport_count = df.filter(F.col("origin_airport_iata_code") == F.col("dest_airport_iata_code")).count()
    if same_airport_count > 0:
        raise ValueError(f"Quality Gate Falhou: {same_airport_count} voos com origem e destino iguais.")

@quality_check("positive_distance")
def _check_positive_distance(df: DataFrame, **kwargs):
    negative_distance_count = df.filter(F.col("DISTANCE") <= 0).count()
    if negative_distance_count > 0:
        raise ValueError(f"Quality Gate Falhou: {negative_distance_count} voos com distância não positiva.")

@quality_check("delay_consistency")
def _check_delay_consistency(df: DataFrame, **kwargs):
    delay_cols = ["AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]
    df_check = df.filter(F.col("ARRIVAL_DELAY") > 0).withColumn("REASON_SUM", sum(F.col(c) for c in delay_cols))

    inconsistent_count = df_check.filter(F.abs(F.col("ARRIVAL_DELAY") - F.col("REASON_SUM")) > 1.0).count()
    if inconsistent_count > 0:
        raise ValueError(f"Quality Gate Falhou: {inconsistent_count} voos com soma de motivos de atraso inconsistente.")

def run_quality_gates_on_df(df: DataFrame, checks: list, **kwargs):
    """Função principal que executa uma lista de checagens de qualidade em um DataFrame."""

    log.info(f"Executando quality gates: {checks}")

    for check_name in checks:
        if check_name not in _CHECKS:
            raise NotImplementedError(f"A checagem de qualidade '{check_name}' não foi implementada.")
        _CHECKS[check_name](df, **kwargs)

    log.info("Todos os quality gates passaram com sucesso.")
