from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from transformer.utils.logger import get_logger

log = get_logger("generic_helpers")


def to_date_from_ymd(year_col: F.Column, month_col: F.Column, day_col: F.Column) -> F.Column:
    """
    Constrói uma coluna de data ('yyyy-MM-dd') a partir de colunas separadas de ano, mês e dia.

    Args:
        year_col (F.Column): Coluna representando o ano.
        month_col (F.Column): Coluna representando o mês.
        day_col (F.Column): Coluna representando o dia.

    Returns:
        F.Column: Coluna formatada como data no padrão 'yyyy-MM-dd'.

    Raises:
        TypeError: Se algum argumento não for uma instância de 'pyspark.sql.Column'.
    """
    # Verifica se todos os argumentos são colunas Spark
    if not all(isinstance(c, F.Column) for c in [year_col, month_col, day_col]):
        raise TypeError("Todos os argumentos devem ser instâncias de pyspark.sql.Column.")

    # Monta a coluna de data no formato yyyy-MM-dd
    date_col = F.to_date(
        F.concat_ws(
            "-",
            year_col.cast(StringType()),
            F.lpad(month_col.cast(StringType()), 2, "0"),
            F.lpad(day_col.cast(StringType()), 2, "0"),
        ),
        "yyyy-MM-dd",
    )

    return date_col
