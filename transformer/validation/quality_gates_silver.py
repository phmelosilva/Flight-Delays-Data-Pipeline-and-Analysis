from pyspark.sql import DataFrame, functions as F
from transformer.utils.logger import get_logger

log = get_logger("quality_gates_silver")


# Verificação de dataset não vazio
def _check_row_count_not_empty(df: DataFrame, name: str) -> None:
    """
    Valida que o DataFrame contém pelo menos um registro.

    Args:
        df (DataFrame): DataFrame a ser verificado.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se o DataFrame estiver vazio.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"[Quality] {name}: dataset vazio.")
    log.info(f"[Quality] {name}: dataset não vazio OK.")

# Partida antes da chegada
def _check_departure_before_arrival(df: DataFrame, name: str) -> None:
    """
    Verifica se o horário de partida ocorre antes do horário de chegada. Registros com horários nulos são ignorados.

    Args:
        df (DataFrame): DataFrame contendo as colunas 'departure_time' e 'arrival_time'.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se houver registros com partida posterior ou igual à chegada.
    """
    invalid = df.filter(
        F.col("departure_time").isNotNull()
        & F.col("arrival_time").isNotNull()
        & (F.col("departure_time") >= F.col("arrival_time"))
    ).count()
    if invalid > 0:
        raise ValueError(f"[Quality] {name}: {invalid:,} voos com partida >= chegada.")

    log.info(f"[Quality] {name}: horários de partida/chegada OK.")


# Origem e destino diferentes
def _check_origin_dest_different(df: DataFrame, name: str) -> None:
    """
    Verifica se o aeroporto de origem e destino não são iguais. Compatível com colunas 
    da silver ('origin_airport', 'destination_airport').

    Args:
        df (DataFrame): DataFrame contendo as colunas de origem e destino.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se existirem registros com origem e destino idênticos.
    """
    same = df.filter(
        F.col("origin_airport").isNotNull()
        & F.col("destination_airport").isNotNull()
        & (F.col("origin_airport") == F.col("destination_airport"))
    ).count()
    if same > 0:
        raise ValueError(f"[Quality] {name}: {same:,} voos com origem == destino.")

    log.info(f"[Quality] {name}: colunas de origem/destino OK.")


# Distância positiva
def _check_positive_distance(df: DataFrame, name: str) -> None:
    """
    Verifica se todos os valores de distância são positivos.

    Registros sem valor informado são ignorados.

    Args:
        df (DataFrame): DataFrame contendo a coluna 'distance'.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se existirem distâncias menores ou iguais a zero.
    """
    invalid = df.filter(F.col("distance").isNotNull() & (F.col("distance") <= 0)).count()
    if invalid > 0:
        raise ValueError(f"[Quality] {name}: {invalid:,} voos com distância não positiva.")

    log.info(f"[Quality] {name}: distâncias positivas OK.")


# Consistência dos atrasos
def _check_delay_consistency(df: DataFrame, name: str) -> None:
    """
    Verifica se a soma dos motivos de atraso é consistente com o valor total de arrival_delay.Uma 
    tolerância de +-5 minutos é permitida para compensar arredondamentos e registros incompletos.

    Args:
        df (DataFrame): DataFrame contendo arrival_delay e colunas de atraso específicas.
        name (str): Identificador lógico do dataset em validação.

    Raises:
        ValueError: Se a diferença entre arrival_delay e a soma dos motivos exceder 5 minutos.
    """
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

    log.info(f"[Quality] {name}: consistência dos atrasos OK.")


# Integridade referencial
def _check_referential_integrity(
    df: DataFrame,
    right_df: DataFrame,
    left_key: str,
    right_key: str,
    left_name: str,
    right_name: str,
) -> None:
    """
    Valida a integridade referencial entre dois DataFrames.

    Args:
        df (DataFrame): DataFrame principal (lado esquerdo do join).
        right_df (DataFrame): DataFrame de referência (lado direito do join).
        left_key (str): Coluna fk no DataFrame principal.
        right_key (str): Coluna pk no DataFrame de referência.
        left_name (str): Nome lógico do DataFrame principal.
        right_name (str): Nome lógico do DataFrame de referência.

    Raises:
        ValueError: Se existirem chaves órfãs no DataFrame principal.
    """
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

    log.info(f"[Quality] {left_name}: integridade FK '{left_key}' → {right_name}.{right_key} OK.")


# Executor
def run_quality_gates_silver(
    flights_df: DataFrame,
    airports_df: DataFrame,
) -> None:
    """
    Executa o conjunto de verificações de qualidade da camada silver.

    Args:
        flights_df (DataFrame): Dataset de voos tratados.
        airports_df (DataFrame): Dataset de aeroportos.

    Raises:
        ValueError: Se qualquer verificação de qualidade falhar.
    """
    log.info("[Quality] Iniciando validações da camada silver.")

    _check_row_count_not_empty(flights_df, "flights_silver")
    _check_departure_before_arrival(flights_df, "flights_silver")
    _check_origin_dest_different(flights_df, "flights_silver")
    _check_positive_distance(flights_df, "flights_silver")
    _check_delay_consistency(flights_df, "flights_silver")
    _check_referential_integrity(
        flights_df,
        airports_df,
        left_key="origin_airport",
        right_key="iata_code",
        left_name="flights_silver",
        right_name="airports_silver",
    )

    log.info("[Quality] Todas as validações da silver concluídas com sucesso.")
