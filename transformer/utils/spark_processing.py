from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def save_df_as_parquet_file(df: DataFrame, dest_path: str, filename: str, single_file: bool = False) -> str:
    """
    Salva um DataFrame Spark em formato Parquet.

    Por padrão, os dados são salvos de forma particionada (vários arquivos part-*.parquet).
    Caso seja necessário gerar um único arquivo parquet consolidado, utilize o parâmetro
    `single_file=True`.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        dest_path (str): Diretório de destino.
        filename (str): Nome da subpasta onde os dados serão gravados.
        single_file (bool): Se True, força a escrita em apenas um arquivo parquet (coalesce=1).

    Returns:
        str: Caminho final em que o DataFrame foi salvo.

    Raises:
        IOError: Se ocorrer falha ao deletar diretório existente antes da escrita.
        Exception: Se ocorrer qualquer falha inesperada durante a escrita do Parquet.
    """
    final_output_dir = f"{dest_path}/{filename}"

    fs = df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(
        df.sparkSession._jsc.hadoopConfiguration()
    )
    Path = df.sparkSession._jvm.org.apache.hadoop.fs.Path

    if fs.exists(Path(final_output_dir)):
        if not fs.delete(Path(final_output_dir), True):
            raise IOError(f"Não foi possível limpar o diretório existente: {final_output_dir}")

    try:
        df_to_write = df.coalesce(1) if single_file else df
        df_to_write.write.mode("overwrite").parquet(final_output_dir)

        log.info(
            f"DataFrame salvo em formato parquet em '{final_output_dir}'."
        )

        return final_output_dir

    except Exception as e:
        log.error(f"Falha ao salvar DataFrame em '{final_output_dir}': {e}")
        raise
