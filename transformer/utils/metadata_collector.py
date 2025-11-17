from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

log = LoggingMixin().log


def log_files_metadata(spark: SparkSession, bronze_path: str, processing_date: str, output_csv_path: str):
    """
    Inspeciona os arquivos na camada Bronze, coleta metadados (nº de linhas, tamanho)
    e salva essas informações em um arquivo CSV.
    """
    log.info("Iniciando a coleta de metadados da camada Bronze...")

    base_path = f"{bronze_path}/{processing_date}"
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    PathHadoop = spark._jvm.org.apache.hadoop.fs.Path

    format_dirs = fs.listStatus(PathHadoop(base_path))
    metadata_list = []

    for format_dir_status in format_dirs:
        format_path = format_dir_status.getPath()
        file_format = format_path.getName().upper()

        data_files_status = fs.listStatus(format_path)
        for data_file_status in data_files_status:
            data_path_obj = data_file_status.getPath()
            data_path = str(data_path_obj)
            file_name = data_path_obj.getName()

            log.info(f"Coletando metadados para: {file_name}")

            df = spark.read.format(file_format.lower()).option("header", "true").load(data_path)
            row_count = df.count()

            content_summary = fs.getContentSummary(data_path_obj)
            file_size_bytes = content_summary.getLength()

            metadata_list.append({
                "file_name": file_name,
                "file_path": data_path,
                "file_format": file_format,
                "row_count": row_count,
                "file_size_bytes": file_size_bytes,
                "ingestion_date": processing_date
            })

    if not metadata_list:
        log.warning("Nenhum metadado coletado.")
        return

    log.info(f"Salvando {len(metadata_list)} registros de metadados...")
    metadata_df = pd.DataFrame(metadata_list)

    output_file = Path(output_csv_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    metadata_df.to_csv(output_file, index=False)

    log.info("Metadados salvos com sucesso.")
