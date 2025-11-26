from pathlib import Path
from pyspark.sql import SparkSession
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd

log = LoggingMixin().log


def log_files_metadata(
    spark: SparkSession,
    bronze_path: str,
    processing_date: str,
    output_csv_path: str,
) -> None:
    """
    Inspeciona os arquivos da camada Bronze, coleta metadados e salva um CSV
    contendo informações úteis para auditoria:
        - nome do arquivo
        - caminho completo
        - formato
        - contagem de linhas
        - tamanho em bytes
        - data de ingestão

    A leitura e inspeção são realizadas diretamente via Hadoop FileSystem.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        bronze_path (str): Caminho base da camada Bronze.
        processing_date (str): Partição de data (YYYY-MM-DD).
        output_csv_path (str): Caminho completo do CSV de saída.

    Raises:
        Exception: Para falhas inesperadas na leitura ou escrita de metadados.
    """
    try:
        log.info("Iniciando coleta de metadados da camada Bronze.")

        base_path = f"{bronze_path}/{processing_date}"

        # Acesso ao Hadoop FS via JVM
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        HPath = spark._jvm.org.apache.hadoop.fs.Path

        format_dirs = fs.listStatus(HPath(base_path))
        metadata_list = []

        # Itera sobre diretórios de formato (ex.: PARQUET, CSV)
        for format_dir in format_dirs:
            format_path = format_dir.getPath()
            file_format = format_path.getName().upper()

            # Arquivos dentro de cada formato
            data_files = fs.listStatus(format_path)

            for file_status in data_files:
                data_path_obj = file_status.getPath()
                data_path = str(data_path_obj)
                file_name = data_path_obj.getName()

                log.info(f"Coletando metadados: {file_name}")

                # Leitura do arquivo para contagem de linhas
                df = (
                    spark.read.format(file_format.lower())
                    .option("header", "true")
                    .load(data_path)
                )
                row_count = df.count()

                # Tamanho físico do arquivo
                size_bytes = fs.getContentSummary(data_path_obj).getLength()

                metadata_list.append(
                    {
                        "file_name": file_name,
                        "file_path": data_path,
                        "file_format": file_format,
                        "row_count": row_count,
                        "file_size_bytes": size_bytes,
                        "ingestion_date": processing_date,
                    }
                )

        if not metadata_list:
            log.warning("Nenhum metadado encontrado.")
            return

        # Conversão para Pandas e persistência
        log.info(f"Salvando {len(metadata_list)} registros de metadados.")
        metadata_df = pd.DataFrame(metadata_list)

        output_file = Path(output_csv_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        metadata_df.to_csv(output_file, index=False)

        log.info("Metadados salvos com sucesso.")

    except Exception as e:
        log.error(f"[ERROR] Falha durante coleta de metadados Bronze: {e}")
        raise
