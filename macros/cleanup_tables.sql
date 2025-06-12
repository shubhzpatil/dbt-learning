{% macro clean_unused_tables(dataset_id,days) %}

    {% set sql %}
        SELECT
            'NAME' AS dummy_column
    {% endset %}

    {{ log('Generating drop statemtnts by executing query') }}
    {% set drop_statements = run_query(sql).columns[0].values() %}

{% endmacro %}