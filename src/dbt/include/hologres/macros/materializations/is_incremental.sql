{% macro is_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {% set is_incremental_result = (relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'incremental'
                  and not should_full_refresh()) %}

        {% if not is_incremental_result
              and relation is not none
              and relation.type == 'table'
              and model.config.materialized == 'logical_partition_table'
              and config.get('incremental_strategy', 'partition') == 'partition'
              and not should_full_refresh() %}
            {% set raw_incremental_partition_key = config.get('incremental_partition_key', none) %}
            {% if raw_incremental_partition_key is not none %}
                {% set logical_partition_columns = hologres__parse_partition_keys(
                    config.get('logical_partition_key', none),
                    'logical_partition_key',
                    required=true) %}
                {% set incremental_partition_columns = hologres__parse_partition_keys(
                    raw_incremental_partition_key,
                    'incremental_partition_key') %}
                {% do hologres__validate_incremental_partition_keys(
                    logical_partition_columns, incremental_partition_columns) %}
                {% set is_incremental_result = true %}
            {% endif %}
        {% endif %}

        {{ return(is_incremental_result) }}
    {% endif %}
{% endmacro %}
