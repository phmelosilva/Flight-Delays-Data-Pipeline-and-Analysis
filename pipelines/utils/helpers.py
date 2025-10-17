from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def to_date_from_ymd(year_col: F.Column, month_col: F.Column, day_col: F.Column) -> F.Column:
    """
    Retorna uma coluna Date construída a partir de YEAR, MONTH, DAY.
    """
    return F.to_date(
        F.concat_ws(
            "-",
            year_col.cast(StringType()),
            F.lpad(month_col.cast(StringType()), 2, "0"),
            F.lpad(day_col.cast(StringType()), 2, "0"),
        ),
        "yyyy-MM-dd",
    )