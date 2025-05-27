{% macro cent_to_dollers(column_name) -%}
{{ column_name }} / 100
{%- endmacro %}