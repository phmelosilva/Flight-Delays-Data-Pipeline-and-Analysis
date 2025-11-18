from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from transformer.utils.logger import get_logger

log = get_logger("generic_helpers")


def to_date_from_ymd(year_col: F.Column, month_col: F.Column, day_col: F.Column) -> F.Column:
    """
    Constrói uma coluna de data ('yyyy-MM-dd') a partir de colunas separadas de ano, mês e dia.
    Essa função é útil para padronizar datas em datasets que armazenam componentes de data
    de forma fragmentada (ex.: YEAR, MONTH, DAY). O formato resultante é compatível com o
    tipo 'DateType' do Spark e pode ser utilizado em joins e comparações temporais.

    Args:
        year_col (F.Column): Coluna representando o ano.
        month_col (F.Column): Coluna representando o mês.
        day_col (F.Column): Coluna representando o dia.

    Returns:
        F.Column: Coluna Spark formatada como data no padrão 'yyyy-MM-dd'.

    Raises:
        TypeError: Se algum dos parâmetros não for uma instância de 'pyspark.sql.Column'.
    """
    try:
        # Validação de tipo
        if not all(isinstance(c, F.Column) for c in [year_col, month_col, day_col]):
            raise TypeError("[ERROR] Todos os argumentos devem ser colunas do Spark (F.Column).")

        log.info("[INFO] Construindo coluna de data a partir de YEAR, MONTH e DAY.")

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

    except Exception as e:
        log.error(f"[ERROR] Falha ao construir coluna de data: {e}.")
        raise
