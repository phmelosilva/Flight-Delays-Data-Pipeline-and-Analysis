-- Test: not_empty
-- Verifica se o modelo possui pelo menos uma linha.
{% test not_empty(model) %}
    SELECT COUNT(*) AS row_count
    FROM {{ model }}
    HAVING COUNT(*) = 0
{% endtest %}


-- Test: required_columns
-- Valida que todas as colunas obrigatórias existem no modelo.
{% test required_columns(model, columns) %}

    {%- set existing_cols = adapter.get_columns_in_relation(model) | map(attribute='name') | list -%}
    {%- set existing_lower = existing_cols | map('lower') | list -%}

    WITH required AS (
        SELECT UNNEST(ARRAY[
            {%- for col in columns %}
                '{{ col | lower }}'{% if not loop.last %}, {% endif %}
            {%- endfor %}
        ]) AS required_col
    )
    SELECT required_col AS missing_column
    FROM required
    WHERE required_col NOT IN (
        {%- for col in existing_lower %}
            '{{ col }}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
    )

{% endtest %}


-- Test: pk_not_null
-- Garante que nenhuma coluna da PK possua valores nulos.
{% test pk_not_null(model, pk_columns) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {%- for col in pk_columns %}
            {{ col }} IS NULL{% if not loop.last %} OR{% endif %}
        {%- endfor %}
{% endtest %}


-- Test: pk_unique
-- Valida unicidade da chave primária.
{% test pk_unique(model, pk_columns) %}
    SELECT
        {%- for col in pk_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        COUNT(*) AS cnt
    FROM {{ model }}
    GROUP BY
        {%- for col in pk_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    HAVING COUNT(*) > 1
{% endtest %}


-- Test: no_full_duplicates
-- Verifica se o conjunto de colunas informado não possui duplicatas completas.
{% test no_full_duplicates(model, subset_columns) %}
    SELECT
        {%- for col in subset_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        COUNT(*) AS cnt
    FROM {{ model }}
    GROUP BY
        {%- for col in subset_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    HAVING COUNT(*) > 1
{% endtest %}


-- Test: departure_before_arrival
-- Verifica se a partida ocorre antes da chegada.
{% test departure_before_arrival(model, departure_col, arrival_col) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {{ departure_col }} IS NOT NULL
        AND {{ arrival_col }}   IS NOT NULL
        AND {{ arrival_col }} <= {{ departure_col }}
{% endtest %}



-- Test: origin_dest_different
-- Verifica que origem e destino não são iguais.
{% test origin_dest_different(model, origin_col, dest_col) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ origin_col }} IS NOT NULL
      AND {{ dest_col }}   IS NOT NULL
      AND {{ origin_col }} = {{ dest_col }}
{% endtest %}


-- Test: distance_positive
-- Valida que a coluna de distância, quando existente, possui valores positivos.
{% test distance_positive(model, column_name) %}

    {%- set cols = adapter.get_columns_in_relation(model) | map(attribute='name') | list -%}
    {%- set cols_lower = cols | map('lower') | list -%}

    {%- if column_name | lower not in cols_lower %}
        SELECT 1 WHERE 1 = 0
    {%- else %}
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
          AND {{ column_name }} <= 0
    {%- endif %}

{% endtest %}


-- Test: delay_consistency
-- Verifica consistência entre arrival_delay e a soma dos motivos de atraso.
{% test delay_consistency(model, arrival_delay, detail_cols) %}
    SELECT *
    FROM (
        SELECT
            *,
            (
                {%- for col in detail_cols %}
                    COALESCE({{ col }}, 0.0){% if not loop.last %} + {% endif %}
                {%- endfor %}
            ) AS reason_sum
        FROM {{ model }}
        WHERE {{ arrival_delay }} > 0
    ) AS sub
    WHERE reason_sum > 0
      AND ABS({{ arrival_delay }} - reason_sum) > 5.0
{% endtest %}


-- Test: fk_exists
-- Valida integridade referencial entre duas relações.
{% test fk_exists(model, column, fk_model, fk_column) %}
    WITH left_keys AS (
        SELECT DISTINCT {{ column }} AS key
        FROM {{ model }}
        WHERE {{ column }} IS NOT NULL
    ),
    right_keys AS (
        SELECT DISTINCT {{ fk_column }} AS key
        FROM {{ fk_model }}
        WHERE {{ fk_column }} IS NOT NULL
    ),
    missing AS (
        SELECT l.key
        FROM left_keys l
        LEFT JOIN right_keys r
          ON l.key = r.key
        WHERE r.key IS NULL
    )
    SELECT *
    FROM missing
{% endtest %}


-- Test: not_null_columns
-- Garante que todas as colunas informadas não possuam valores nulos.
{% test not_null_columns(model, columns) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {%- for col in columns %}
            {{ col }} IS NULL{% if not loop.last %} OR{% endif %}
        {%- endfor %}
{% endtest %}


-- Test: unique_column
-- Verifica unicidade de uma coluna individual.
{% test unique_column(model, column_name) %}
    SELECT {{ column_name }}, COUNT(*) AS cnt
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
{% endtest %}
