{% macro show_num(num) %}
    { set numbers = num}
    {{ return numbers}}
{% endmacro %}