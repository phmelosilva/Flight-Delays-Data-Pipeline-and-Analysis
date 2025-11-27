-- Model: dim_dat
-- Descrição: Dimensão de datas construída a partir da tabela OBT.


{{ config(
    materialized = "table",
    schema       = "dbt_gold",
    tags         = ["gold", "dim", "date"]
) }}

-- Lista de feriados dos EUA em 2015 (USDOT / calendário federal)
{% set us_holidays_2015 = [
    "2015-01-01",
    "2015-01-19",
    "2015-02-16",
    "2015-05-25",
    "2015-07-04",
    "2015-09-07",
    "2015-10-12",
    "2015-11-11",
    "2015-11-26",
    "2015-12-25"
] %}

-- Datas distintas presentes nos voos da silver
WITH base AS (
    SELECT DISTINCT
        flight_date
    FROM {{ ref('silver_flights') }}
    WHERE flight_date IS NOT NULL
),

-- Construção dos atributos da dimensão de datas
final AS (
    SELECT
        flight_date                                                    AS full_date,
        EXTRACT(YEAR    FROM flight_date)::SMALLINT                    AS year,
        EXTRACT(MONTH   FROM flight_date)::SMALLINT                    AS month,
        EXTRACT(DAY     FROM flight_date)::SMALLINT                    AS day,
        (((EXTRACT(DOW  FROM flight_date)::INT + 6) % 7) + 1)::SMALLINT AS day_of_week,
        EXTRACT(QUARTER FROM flight_date)::SMALLINT                    AS quarter,
        (
            flight_date::DATE IN (
                {% for d in us_holidays_2015 %}
                    '{{ d }}'{{ "," if not loop.last }}
                {% endfor %}
            )
        ) AS is_holiday
    FROM base
)

-- Seleção ordenada para formar a dimensão
SELECT *
FROM final
ORDER BY full_date
