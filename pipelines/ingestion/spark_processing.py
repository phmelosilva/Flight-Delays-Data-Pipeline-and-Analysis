from pyspark.sql import SparkSession, DataFrame
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def reassemble_chunks(spark: SparkSession, chunk_files: list[str], header: bool = True) -> DataFrame:
    """
    Lê múltiplos chunks (arquivos csv) e unifica em um único DataFrame.

    Args:
        spark (SparkSession): A sessão Spark ativa, fornecida pelo Airflow.
        chunk_files (list[str]): Caminhos para os arquivos csv a serem unificados.
        header (bool): Se os arquivos csv contêm cabeçalho.

    Returns:
        DataFrame: DataFrame Spark com os dados unificados.

    Raises:
        ValueError: Se a lista de arquivos estiver vazia.
    """
    if not chunk_files:
        raise ValueError("Nenhum chunk fornecido.")

    log.info(f"Unificando {len(chunk_files)} chunks.")

    df = spark.read.option("header", header).csv(chunk_files)

    log.info("Unificação dos chunks concluída com sucesso.")

    return df


def save_df_as_single_file(df: DataFrame, dest_path: str, filename: str, header: bool = True) -> None:
    """
    Salva um DataFrame Spark como um único arquivo CSV.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        dest_path (str): Diretório de destino.
        filename (str): Nome final do arquivo.
        header (bool): Se o CSV final deve conter cabeçalho.

    Raises:
        FileNotFoundError: Se nenhum arquivo part-* for gerado pelo Spark.
    """
    temp_output_dir = f"{dest_path}/_temp_output"

    fs = df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(df.sparkSession._jsc.hadoopConfiguration())
    Path = df.sparkSession._jvm.org.apache.hadoop.fs.Path

    try:
        df.coalesce(1).write.mode("overwrite").option("header", header).csv(temp_output_dir)

        part_files = fs.globStatus(Path(f"{temp_output_dir}/part-*.csv"))
        if not part_files:
            raise FileNotFoundError("Nenhum arquivo gerado pelo Spark.")

        part_files = sorted(part_files, key=lambda f: f.getModificationTime(), reverse=True)
        source_file_path = part_files[0].getPath()
        final_file_path = Path(f"{dest_path}/{filename}")

        log.info(f"Renomeando '{source_file_path.getName()}' para '{final_file_path.getName()}'.")

        if fs.exists(final_file_path):
            fs.delete(final_file_path, True)

        fs.rename(source_file_path, final_file_path)

        log.info(f"Arquivo '{filename}' salvo com sucesso em '{dest_path}'.")

    finally:
        if fs.exists(Path(temp_output_dir)):
            fs.delete(Path(temp_output_dir), True)
