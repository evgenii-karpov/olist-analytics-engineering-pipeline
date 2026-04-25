{% macro round_two_decimals(expression) -%}
    round({{ expression }}, 2)
{%- endmacro %}
