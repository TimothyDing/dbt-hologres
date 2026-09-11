"""Integration tests for incremental logical partition tables.

These tests run real dbt invocations against Hologres and verify both result
data and logical-partition metadata.

Run with:
    DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true pytest tests/integration/test_logical_partition_incremental.py
"""

import os
from pathlib import Path

import psycopg


def create_model_file(project_dir: Path, model_name: str, sql_content: str) -> Path:
    model_path = project_dir / "models" / f"{model_name}.sql"
    model_path.write_text(sql_content)
    return model_path


def fetch_rows(schema_name: str, table_name: str, query: str):
    with psycopg.connect(
        host=os.getenv("DBT_HOLOGRES_HOST"),
        port=int(os.getenv("DBT_HOLOGRES_PORT", "80")),
        user=os.getenv("DBT_HOLOGRES_USER"),
        password=os.getenv("DBT_HOLOGRES_PASSWORD"),
        dbname=os.getenv("DBT_HOLOGRES_DATABASE"),
    ) as connection, connection.cursor() as cursor:
        cursor.execute(query.format(schema=schema_name, table=table_name))
        return cursor.fetchall()


def assert_logical_partitioned(schema_name: str, table_name: str):
    rows = fetch_rows(
        schema_name,
        table_name,
        """
        select count(*)
        from hologres.hg_table_properties
        where table_namespace = '{schema}'
          and table_name = '{table}'
          and property_key = 'is_logical_partitioned_table'
          and property_value in ('true', 't')
        """,
    )
    assert rows == [(1,)]


class TestLogicalPartitionIncrementalIntegration:
    def test_single_key_replaces_only_touched_partition(
        self,
        dbt_project_dir: Path,
        dbt_runner,
        cleanup_schema,
        unique_schema_name: str,
    ):
        model_path = create_model_file(
            dbt_project_dir,
            "single_partition_incremental",
            """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    incremental_strategy='partition'
) }}

select date '2024-01-01' as ds, 1::bigint as id, 'history'::text as value
union all
select date '2024-01-02' as ds, 2::bigint as id, 'old'::text as value
""",
        )

        result = dbt_runner.invoke(["run"])
        assert result.success, f"initial dbt run failed: {result.exception}"

        model_path.write_text(
            """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    incremental_strategy='partition'
) }}

select date '2024-01-02' as ds, 3::bigint as id, 'new'::text as value
"""
        )

        result = dbt_runner.invoke(["run"])
        assert result.success, f"incremental dbt run failed: {result.exception}"

        rows = fetch_rows(
            unique_schema_name,
            "single_partition_incremental",
            """
            select ds::text, id, value
            from {schema}.{table}
            order by ds, id
            """,
        )
        assert rows == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 3, "new"),
        ]
        assert_logical_partitioned(
            unique_schema_name,
            "single_partition_incremental",
        )

    def test_dual_key_replaces_only_touched_composite_partition(
        self,
        dbt_project_dir: Path,
        dbt_runner,
        cleanup_schema,
        unique_schema_name: str,
    ):
        model_path = create_model_file(
            dbt_project_dir,
            "dual_partition_incremental",
            """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='yy, mm',
    incremental_partition_key='yy, mm',
    incremental_strategy='partition'
) }}

select 2024::integer as yy, 1::integer as mm, 1::bigint as id, 'jan-old'::text as value
union all
select 2024::integer as yy, 2::integer as mm, 2::bigint as id, 'feb-history'::text as value
""",
        )

        result = dbt_runner.invoke(["run"])
        assert result.success, f"initial dbt run failed: {result.exception}"

        model_path.write_text(
            """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='yy, mm',
    incremental_partition_key='yy, mm',
    incremental_strategy='partition'
) }}

select 2024::integer as yy, 1::integer as mm, 3::bigint as id, 'jan-new'::text as value
"""
        )

        result = dbt_runner.invoke(["run"])
        assert result.success, f"incremental dbt run failed: {result.exception}"

        rows = fetch_rows(
            unique_schema_name,
            "dual_partition_incremental",
            """
            select yy, mm, id, value
            from {schema}.{table}
            order by yy, mm, id
            """,
        )
        assert rows == [
            (2024, 1, 3, "jan-new"),
            (2024, 2, 2, "feb-history"),
        ]
        assert_logical_partitioned(
            unique_schema_name,
            "dual_partition_incremental",
        )
