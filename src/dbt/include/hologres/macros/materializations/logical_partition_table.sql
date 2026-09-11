{% materialization logical_partition_table, adapter='hologres' -%}

  {%- set logical_partition_columns = hologres__parse_partition_keys(
        config.get('logical_partition_key', none),
        'logical_partition_key',
        required=true) -%}
  {%- set incremental_partition_columns = hologres__parse_partition_keys(
        config.get('incremental_partition_key', none),
        'incremental_partition_key') -%}
  {%- if incremental_partition_columns | length > 0 -%}
    {%- do hologres__validate_incremental_partition_keys(
          logical_partition_columns, incremental_partition_columns) -%}
  {%- endif -%}
  {%- set incremental_strategy = config.get('incremental_strategy', 'partition') -%}
  {%- if incremental_strategy != 'partition' -%}
    {% do exceptions.raise_compiler_error("materialized='logical_partition_table' only supports incremental_strategy='partition'") %}
  {%- endif -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set temp_schema_relation = make_temp_relation(intermediate_relation, '_schema') -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set is_partition_incremental = (
        existing_relation is not none
        and existing_relation.type == 'table'
        and incremental_partition_columns | length > 0
        and not should_full_refresh()) -%}
  {%- set full_refresh_mode = not is_partition_incremental -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(
        config.get('on_schema_change'), default='ignore') -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set grant_config = config.get('grants') -%}

  -- remove stale working relations before running hooks
  {%- set preexisting_temp_relation = load_cached_relation(temp_relation) -%}
  {%- set preexisting_temp_schema_relation = load_cached_relation(temp_schema_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {{ drop_relation_if_exists(preexisting_temp_schema_relation) }}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set to_drop = [] -%}

  {% if is_partition_incremental %}
    {# Hologres has no temporary tables. Avoid the logical_partition_key guard
       in get_create_table_as_sql by creating a normal staging table directly. #}
    {% call statement('create_temp_for_incremental', auto_begin=False) -%}
      {{ sql_header if sql_header is not none }}
      create table {{ temp_relation }} as ({{ sql }});
    {%- endcall %}
    {%- do to_drop.append(temp_relation) -%}

    {%- set contract_config = config.get('contract') -%}
    {% if not contract_config or not contract_config.enforced %}
      {% do adapter.expand_target_column_types(
          from_relation=temp_relation,
          to_relation=target_relation) %}
    {% endif %}

    {%- set dest_columns = process_schema_changes(
          on_schema_change, temp_relation, existing_relation) -%}
    {%- if not dest_columns -%}
      {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- endif -%}

    {# Revalidate after schema changes so a removed or renamed partition key
       fails before partition replacement. A changed partition definition
       requires rebuilding the logical partition table. #}
    {%- set source_columns = adapter.get_columns_in_relation(temp_relation) -%}
    {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set refresh_hint = 'incremental_partition_key (use --full-refresh after changing the partition definition)' -%}
    {%- do hologres__validate_partition_columns(
          source_columns, incremental_partition_columns, refresh_hint) -%}
    {%- do hologres__validate_partition_columns(
          target_columns, logical_partition_columns, refresh_hint) -%}
    {%- do hologres__validate_partition_columns(
          dest_columns, incremental_partition_columns, refresh_hint) -%}

    {%- set build_sql = hologres__get_partition_strategy_sql(
          target_relation,
          temp_relation,
          incremental_partition_columns,
          dest_columns) -%}

    {% call statement('main', auto_begin=False) -%}
      {{ build_sql }}
    {%- endcall %}
  {% else %}
    {# Infer the model schema with a normal table, then create the logical
       partition table explicitly. The target remains untouched until swap. #}
    {% call statement('create_temp_for_schema', auto_begin=False) -%}
      {{ sql_header if sql_header is not none }}
      create table {{ temp_schema_relation }} as ({{ sql }}) limit 0;
    {%- endcall %}

    {%- set columns = adapter.get_columns_in_relation(temp_schema_relation) -%}

    {% call statement('drop_temp_for_schema', auto_begin=False) -%}
      drop table if exists {{ temp_schema_relation }};
    {%- endcall %}

    {%- do hologres__validate_partition_columns(
          columns, logical_partition_columns, 'logical_partition_key') -%}
    {%- if incremental_partition_columns | length > 0 -%}
      {%- do hologres__validate_partition_columns(
            columns, incremental_partition_columns, 'incremental_partition_key') -%}
    {%- endif -%}
    {%- set with_properties = hologres__build_table_properties() -%}

    {% call statement('create_logical_partition_table', auto_begin=False) -%}
      {{ hologres__create_logical_partition_table_ddl(
          intermediate_relation,
          columns,
          logical_partition_columns,
          with_properties) }}
    {%- endcall %}

    {% call statement('main', auto_begin=False) -%}
      {{ hologres__insert_into_table(intermediate_relation, sql) }}
    {%- endcall %}

    {% do create_indexes(intermediate_relation) %}

    {% if existing_relation is not none %}
      {%- set existing_relation = load_cached_relation(existing_relation) -%}
      {% if existing_relation is not none %}
        {% do adapter.rename_relation(existing_relation, backup_relation) %}
        {%- do to_drop.append(backup_relation) -%}
      {% endif %}
    {% endif %}
    {% do adapter.rename_relation(intermediate_relation, target_relation) %}
  {% endif %}

  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode) -%}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% for relation in to_drop %}
    {% do adapter.drop_relation(relation) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
