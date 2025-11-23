{% test not_empty(model) %}
    select count(*) as row_count
    from {{ model }}
    having count(*) = 0
{% endtest %}


{% test required_columns(model, columns) %}
    {%- set existing_cols = adapter.get_columns_in_relation(model) | map(attribute='name') | list -%}
    {%- set existing_lower = existing_cols | map('lower') | list -%}

    with missing as (
        select
            unnest(array[
                {%- for col in columns %}
                    '{{ col | lower }}'{% if not loop.last %}, {% endif %}
                {%- endfor %}
            ]) as required_col
    )
    select required_col as missing_column
    from missing
    where required_col not in (
        {%- for col in existing_lower %}
            '{{ col }}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
    )
{% endtest %}



{% test pk_not_null(model, pk_columns) %}
    select *
    from {{ model }}
    where
    (
        {%- for col in pk_columns %}
            {{ col }} is null
            {%- if not loop.last %} or {% endif %}
        {%- endfor %}
    )
{% endtest %}


{% test pk_unique(model, pk_columns) %}
    select
        {%- for col in pk_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        count(*) as cnt
    from {{ model }}
    group by
        {%- for col in pk_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    having count(*) > 1
{% endtest %}


{% test no_full_duplicates(model, subset_columns) %}
    select
        {%- for col in subset_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        count(*) as cnt
    from {{ model }}
    group by
        {%- for col in subset_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    having count(*) > 1
{% endtest %}


{% test departure_before_arrival(model, departure_col, arrival_col) %}
    select *
    from {{ model }}
    where {{ departure_col }} is not null
      and {{ arrival_col }}   is not null
      and {{ departure_col }} >= {{ arrival_col }}
{% endtest %}


{% test origin_dest_different(model, origin_col, dest_col) %}
    select *
    from {{ model }}
    where {{ origin_col }} is not null
      and {{ dest_col }}   is not null
      and {{ origin_col }} = {{ dest_col }}
{% endtest %}


{% test distance_positive(model, column_name) %}
    {%- set cols = adapter.get_columns_in_relation(model) | map(attribute='name') | list -%}
    {%- set cols_lower = cols | map('lower') | list -%}

    {%- if column_name | lower not in cols_lower -%}
        select 1 where 1 = 0
    {%- else -%}
        select *
        from {{ model }}
        where {{ column_name }} is not null
          and {{ column_name }} <= 0
    {%- endif %}
{% endtest %}


{% test delay_consistency(model, arrival_delay, detail_cols) %}
    select *
    from (
        select
            *,
            (
                {%- for col in detail_cols %}
                    coalesce({{ col }}, 0.0)
                    {%- if not loop.last %} + {% endif %}
                {%- endfor %}
            ) as reason_sum
        from {{ model }}
        where {{ arrival_delay }} > 0
    ) sub
    where reason_sum > 0
      and abs({{ arrival_delay }} - reason_sum) > 5.0
{% endtest %}


{% test fk_exists(model, column, fk_model, fk_column) %}
    with left_keys as (
        select distinct {{ column }} as key
        from {{ model }}
        where {{ column }} is not null
    ),
    right_keys as (
        select distinct {{ fk_column }} as key
        from {{ fk_model }}
        where {{ fk_column }} is not null
    ),
    missing as (
        select l.key
        from left_keys l
        left join right_keys r
          on l.key = r.key
        where r.key is null
    )
    select *
    from missing
{% endtest %}


{% test not_null_columns(model, columns) %}
    select *
    from {{ model }}
    where
    (
        {%- for col in columns %}
            {{ col }} is null
            {%- if not loop.last %} or {% endif %}
        {%- endfor %}
    )
{% endtest %}


{% test unique_column(model, column_name) %}
    select {{ column_name }}, count(*) as cnt
    from {{ model }}
    group by {{ column_name }}
    having count(*) > 1
{% endtest %}
