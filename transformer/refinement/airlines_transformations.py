from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def transform_airlines(df: DataFrame) -> DataFrame:
    """
    Realiza as transformações necessárias em 'Airlines' para a camada Silver.

    Args:
        df (DataFrame): DataFrame contendo os dados brutos de companhias aéreas,
            lidos a partir da camada Bronze (Parquet ou CSV).

    Returns:
        DataFrame: DataFrame transformado e padronizado para ingestão na camada Silver.

    Raises:
        TypeError: 
        ValueError: 
    """
    required = {"IATA_CODE", "AIRLINE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Colunas faltando em airlines source: {missing}")

    out = (
        df.withColumnRenamed("IATA_CODE", "airline_iata_code")
        .withColumnRenamed("AIRLINE", "airline_name")
        .withColumn("airline_iata_code", F.col("airline_iata_code").cast(StringType()))
        .withColumn("airline_name", F.col("airline_name").cast(StringType()))
    )

    dup_count = out.groupBy("airline_iata_code").count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError(f"Chave primária airline_iata_code não é única, duplicatas encontradas: {dup_count})")

    log.info("Transformação airlines concluída.")
    return out
