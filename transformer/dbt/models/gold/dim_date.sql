{{ config(
    schema = "dbt_gold",
    materialized = "table",
    tags = ["gold", "dim", "date"]
) }}

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

with base as (
    select distinct
        flight_date
    from {{ ref('silver_flights') }}
    where flight_date is not null
),

final as (
    select
        flight_date                                                     as full_date,
        extract(year    from flight_date)::smallint                     as year,
        extract(month   from flight_date)::smallint                     as month,
        extract(day     from flight_date)::smallint                     as day,
        (((extract(dow from flight_date)::int + 6) % 7) + 1)::smallint  as day_of_week,
        extract(quarter from flight_date)::smallint                     as quarter,
        (
            flight_date::date in (
                {% for d in us_holidays_2015 %}
                    '{{ d }}'{{ "," if not loop.last }}
                {% endfor %}
            )
        ) as is_holiday
    from base
)

select * from final order by full_date
