{% macro union_table_by_prefix(project_name,dataset_name,table_prefix) %}

    {% set tables = dbt_utils.get_relations_by_prefix(database=project_name,schema=dataset_name, prefix=table_prefix) %}

    {% for table in tables %}
       select * from {{ table.schema }}.{{ table.name }}
       {% if not loop.last %}
       union all
       {% endif %}
    {% endfor %}

{% endmacro %}