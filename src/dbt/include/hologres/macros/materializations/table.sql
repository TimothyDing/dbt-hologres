{% materialization table, adapter='hologres' %}

  {%- set incremental_partition_key = config.get('incremental_partition_key', none) -%}
  {%- set incremental_strategy = config.get('incremental_strategy', none) -%}
  {%- if incremental_partition_key is not none or incremental_strategy == 'partition' -%}
    {% do exceptions.raise_compiler_error("incremental_partition_key and incremental_strategy='partition' require materialized='logical_partition_table'") %}
  {%- endif -%}

  {%- set logical_partition_key = config.get('logical_partition_key', none) -%}
  {%- set partition_columns = hologres__parse_partition_keys(logical_partition_key, 'logical_partition_key') -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- if partition_columns | length > 0 -%}
    {%- set temp_for_schema = make_temp_relation(intermediate_relation, '_schema') -%}

    {% call statement('create_temp_for_schema', auto_begin=False) -%}
      create table {{ temp_for_schema }} as ({{ sql }}) limit 0
    {%- endcall %}

    {%- set columns = adapter.get_columns_in_relation(temp_for_schema) -%}

    {% call statement('drop_temp_for_schema', auto_begin=False) -%}
      drop table if exists {{ temp_for_schema }}
    {%- endcall %}

    {%- do hologres__validate_partition_columns(columns, partition_columns, 'logical_partition_key') -%}
    {%- set with_properties = hologres__build_table_properties() -%}

    {% call statement('create_logical_partition_table', auto_begin=False) -%}
      {{ hologres__create_logical_partition_table_ddl(intermediate_relation, columns, partition_columns, with_properties) }}
    {%- endcall %}

    {% call statement('main', auto_begin=False) -%}
      {{ hologres__insert_into_table(intermediate_relation, sql) }}
    {%- endcall %}

  {%- else -%}
    -- build model (Hologres需要在事务外执行CTAS)
    -- 由于Hologres的add_begin_query被禁用，实际上不会开启事务
    {% call statement('main', auto_begin=False) -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
  {%- endif -%}

  {% do create_indexes(intermediate_relation) %}

  -- cleanup
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
