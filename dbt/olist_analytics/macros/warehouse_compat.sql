{% macro dateadd_days(timestamp_expression, days) -%}
    {{ return(adapter.dispatch('dateadd_days', 'olist_analytics')(timestamp_expression, days)) }}
{%- endmacro %}

{% macro default__dateadd_days(timestamp_expression, days) -%}
    ({{ timestamp_expression }} + ({{ days }} * interval '1 day'))
{%- endmacro %}

{% macro redshift__dateadd_days(timestamp_expression, days) -%}
    dateadd(day, {{ days }}, {{ timestamp_expression }})
{%- endmacro %}

{% macro days_between(start_expression, end_expression) -%}
    {{ return(adapter.dispatch('days_between', 'olist_analytics')(start_expression, end_expression)) }}
{%- endmacro %}

{% macro default__days_between(start_expression, end_expression) -%}
    (({{ end_expression }})::date - ({{ start_expression }})::date)
{%- endmacro %}

{% macro redshift__days_between(start_expression, end_expression) -%}
    datediff(day, {{ start_expression }}, {{ end_expression }})
{%- endmacro %}
