{% macro show_num(num) %}
    {% for ele in num %}
        select {{ ele }} as numbers {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% endmacro %}