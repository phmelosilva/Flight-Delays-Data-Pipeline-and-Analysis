
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def transform_airports(df: DataFrame) -> DataFrame:
    """
    Realiza as transformações necessárias em 'Airports' para a camada Silver.

    Args:
        df (DataFrame): DataFrame contendo os dados brutos de aeroportos, lidos da camada Bronze.

    Returns:
        DataFrame: DataFrame transformado e consistente com o schema Silver.

    Raises:
        KeyError: 
        ValueError: 
    """
    required = {"IATA_CODE", "LATITUDE", "LONGITUDE"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Colunas faltando em airports source: {missing}")

    corrections = {
        "ECP": {"LATITUDE": 30.3549, "LONGITUDE": -86.6160},
        "PBG": {"LATITUDE": 44.6895, "LONGITUDE": -68.0448},
        "UST": {"LATITUDE": 42.0703, "LONGITUDE": -87.9539},
    }

    out = df.withColumnRenamed("IATA_CODE", "airport_iata_code")

    out = (
        out.withColumn("airport_iata_code", F.col("airport_iata_code").cast(StringType()))
        .withColumn("LATITUDE", F.col("LATITUDE").cast(DoubleType()))
        .withColumn("LONGITUDE", F.col("LONGITUDE").cast(DoubleType()))
    )

    if "COUNTRY" in out.columns:
        out = out.drop("COUNTRY")

    for code, coords in corrections.items():
        out = out.withColumn(
            "LATITUDE",
            F.when(F.col("airport_iata_code") == code, F.lit(coords["LATITUDE"])).otherwise(F.col("LATITUDE")),
        ).withColumn(
            "LONGITUDE",
            F.when(F.col("airport_iata_code") == code, F.lit(coords["LONGITUDE"])).otherwise(F.col("LONGITUDE")),
        )

    dup_count = out.groupBy("airport_iata_code").count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError(f"Chave primária airport_iata_code não é única, duplicatas encontradas: {dup_count})")

    log.info("Transformação airports concluída.")
    return out
