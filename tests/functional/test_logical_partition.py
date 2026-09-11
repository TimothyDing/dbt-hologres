"""Functional coverage for Hologres logical partition materializations.

Run with a configured Hologres instance:
    DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true pytest tests/functional/test_logical_partition.py
"""

from pathlib import Path

import pytest

from dbt.tests.util import run_dbt, write_file

from tests.functional.fixtures import (
    models__logical_partition_full_rebuild_initial,
    models__logical_partition_full_rebuild_update,
    models__logical_partition_incremental_dual_initial,
    models__logical_partition_incremental_dual_update,
    models__logical_partition_incremental_empty,
    models__logical_partition_incremental_initial,
    models__logical_partition_incremental_update,
    models__logical_partition_is_incremental,
)


def assert_success(results):
    assert len(results) == 1
    assert results[0].status == "success"


def overwrite_model(project, filename, contents):
    write_file(contents, Path(project.project_root), "models", filename)


def fetch_rows(project, model_name, columns, order_by):
    return project.run_sql(
        f"select {columns} from {{schema}}.{model_name} order by {order_by}",
        fetch="all",
    )


def assert_logical_partitioned(project, model_name):
    rows = project.run_sql(
        f"""
        select count(*)
        from hologres.hg_table_properties
        where table_namespace = '{{schema}}'
          and table_name = '{model_name}'
          and property_key = 'is_logical_partitioned_table'
          and property_value in ('true', 't')
        """,
        fetch="all",
    )
    assert rows == [(1,)]


def error_text(result):
    return str(getattr(result, "message", result))


class TestPartitionIncrementalSingleKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"partition_incremental.sql": models__logical_partition_incremental_initial}

    def test_replaces_only_touched_partition_without_duplicates(self, project):
        assert_success(run_dbt(["run"]))
        assert fetch_rows(
            project,
            "partition_incremental",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "old-2"),
            ("2024-01-02", 3, "old-3"),
        ]
        assert_logical_partitioned(project, "partition_incremental")

        overwrite_model(
            project,
            "partition_incremental.sql",
            models__logical_partition_incremental_update,
        )
        assert_success(run_dbt(["run"]))

        assert fetch_rows(
            project,
            "partition_incremental",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "new-2"),
            ("2024-01-02", 4, "new-4"),
        ]
        partition_counts = project.run_sql(
            """
            select ds::text, count(*), count(distinct id)
            from {schema}.partition_incremental
            group by ds
            order by ds
            """,
            fetch="all",
        )
        assert partition_counts == [
            ("2024-01-01", 1, 1),
            ("2024-01-02", 2, 2),
        ]


class TestPartitionIncrementalDualKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dual_partition.sql": models__logical_partition_incremental_dual_initial
        }

    def test_replaces_only_touched_composite_partition(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(
            project,
            "dual_partition.sql",
            models__logical_partition_incremental_dual_update,
        )
        assert_success(run_dbt(["run"]))

        assert fetch_rows(
            project,
            "dual_partition",
            "yy, mm, id, value",
            "yy, mm, id",
        ) == [
            (2024, 1, 3, "jan-new"),
            (2024, 2, 2, "feb-history"),
        ]
        assert_logical_partitioned(project, "dual_partition")


class TestPartitionIncrementalEmptyBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"empty_batch.sql": models__logical_partition_incremental_initial}

    def test_empty_batch_leaves_all_partitions_unchanged(self, project):
        assert_success(run_dbt(["run"]))
        before = fetch_rows(
            project,
            "empty_batch",
            "ds::text, id, value",
            "ds, id",
        )

        overwrite_model(
            project,
            "empty_batch.sql",
            models__logical_partition_incremental_empty,
        )
        assert_success(run_dbt(["run"]))

        after = fetch_rows(
            project,
            "empty_batch",
            "ds::text, id, value",
            "ds, id",
        )
        assert after == before


class TestLogicalPartitionFullRebuild:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "full_rebuild.sql": models__logical_partition_full_rebuild_initial
        }

    def test_missing_incremental_key_rebuilds_the_whole_table(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(
            project,
            "full_rebuild.sql",
            models__logical_partition_full_rebuild_update,
        )
        assert_success(run_dbt(["run"]))

        assert fetch_rows(
            project,
            "full_rebuild",
            "ds::text, id, value",
            "ds, id",
        ) == [("2024-02-01", 10, "rebuilt")]
        assert_logical_partitioned(project, "full_rebuild")


class TestLogicalPartitionIsIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_branch.sql": models__logical_partition_is_incremental}

    def test_second_run_uses_incremental_branch_and_full_refresh_does_not(self, project):
        assert_success(run_dbt(["run"]))
        assert_success(run_dbt(["run"]))
        assert fetch_rows(
            project,
            "incremental_branch",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "incremental"),
        ]

        assert_success(run_dbt(["run", "--full-refresh"]))
        assert fetch_rows(
            project,
            "incremental_branch",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "initial"),
        ]


class TestLogicalPartitionViewMigration:
    view_model = """
{{ config(materialized='view') }}
select date '2024-01-01' as ds, 1::bigint as id, 'view'::text as value
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"view_migration.sql": self.view_model}

    def test_view_is_safely_replaced_by_logical_partition_table(self, project):
        assert_success(run_dbt(["run"]))
        assert project.get_tables_in_schema()["view_migration"] == "view"

        overwrite_model(
            project,
            "view_migration.sql",
            models__logical_partition_incremental_initial,
        )
        assert_success(run_dbt(["run"]))

        assert project.get_tables_in_schema()["view_migration"] == "table"
        assert fetch_rows(
            project,
            "view_migration",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "old-2"),
            ("2024-01-02", 3, "old-3"),
        ]
        assert_logical_partitioned(project, "view_migration")
        assert set(project.get_tables_in_schema()) == {"view_migration"}


class TestLogicalPartitionSafeSwap:
    invalid_rebuild = """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds'
) }}
select 'east'::text as region, 99::bigint as id, 'invalid'::text as value
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"safe_swap.sql": models__logical_partition_full_rebuild_initial}

    def test_failed_rebuild_keeps_existing_target_data(self, project):
        assert_success(run_dbt(["run"]))
        before = fetch_rows(
            project,
            "safe_swap",
            "ds::text, id, value",
            "ds, id",
        )

        overwrite_model(project, "safe_swap.sql", self.invalid_rebuild)
        results = run_dbt(["run"], expect_pass=False)

        assert len(results) == 1
        assert results[0].status == "error"
        assert "logical_partition_key column 'ds' is not present" in error_text(
            results[0]
        )
        assert fetch_rows(
            project,
            "safe_swap",
            "ds::text, id, value",
            "ds, id",
        ) == before


class TestLogicalPartitionProperties:
    initial_model = """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    orientation='column',
    distribution_key='id',
    bitmap_columns='id'
) }}
select date '2024-01-01' as ds, 1::bigint as id, 'initial'::text as value
"""

    update_model = initial_model.replace("'2024-01-01'", "'2024-01-02'").replace(
        "'initial'", "'updated'"
    )

    @pytest.fixture(scope="class")
    def models(self):
        return {"partition_properties.sql": self.initial_model}

    def test_incremental_refresh_preserves_table_properties(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(project, "partition_properties.sql", self.update_model)
        assert_success(run_dbt(["run"]))

        assert fetch_rows(
            project,
            "partition_properties",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "initial"),
            ("2024-01-02", 1, "updated"),
        ]
        dump_script = project.run_sql(
            "select hg_dump_script('\"{schema}\".\"partition_properties\"')",
            fetch="all",
        )[0][0].lower()
        assert "orientation = 'column'" in dump_script
        assert "distribution_key = 'id'" in dump_script
        assert "bitmap_columns = 'id'" in dump_script


class TestLogicalPartitionSchemaChange:
    initial_model = """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    on_schema_change='append_new_columns'
) }}
select date '2024-01-01' as ds, 1::bigint as id
"""

    update_model = """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    on_schema_change='append_new_columns'
) }}
select date '2024-01-02' as ds, 2::bigint as id, 'added'::text as extra
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema_change.sql": self.initial_model}

    def test_append_new_columns_keeps_history_and_loads_new_column(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(project, "schema_change.sql", self.update_model)
        assert_success(run_dbt(["run"]))

        columns = project.run_sql(
            """
            select column_name
            from information_schema.columns
            where table_schema = '{schema}'
              and table_name = 'schema_change'
            order by ordinal_position
            """,
            fetch="all",
        )
        assert columns == [("ds",), ("id",), ("extra",)]
        assert fetch_rows(
            project,
            "schema_change",
            "ds::text, id, extra",
            "ds, id",
        ) == [
            ("2024-01-01", 1, None),
            ("2024-01-02", 2, "added"),
        ]


class TestLogicalPartitionDefinitionChange:
    changed_partition_key = """
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='region',
    incremental_partition_key='region'
) }}
select 'east'::text as region, 2::bigint as id, 'changed'::text as value
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"partition_change.sql": models__logical_partition_incremental_initial}

    def test_partition_key_change_requires_full_refresh(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(
            project,
            "partition_change.sql",
            self.changed_partition_key,
        )
        results = run_dbt(["run"], expect_pass=False)

        assert len(results) == 1
        assert results[0].status == "error"
        message = error_text(results[0])
        assert "logical partition columns ds do not exactly match" in message
        assert "--full-refresh" in message
        assert fetch_rows(
            project,
            "partition_change",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-01", 1, "history"),
            ("2024-01-02", 2, "old-2"),
            ("2024-01-02", 3, "old-3"),
        ]


class TestLegacyLogicalPartitionTable:
    initial_model = models__logical_partition_incremental_initial.replace(
        "materialized='logical_partition_table',\n", "materialized='table',\n"
    ).replace("    incremental_partition_key='ds'\n", "")
    update_model = models__logical_partition_incremental_update.replace(
        "materialized='logical_partition_table',\n", "materialized='table',\n"
    ).replace("    incremental_partition_key='ds'\n", "")

    @pytest.fixture(scope="class")
    def models(self):
        return {"legacy_table.sql": self.initial_model}

    def test_legacy_table_configuration_remains_full_rebuild_compatible(self, project):
        assert_success(run_dbt(["run"]))
        overwrite_model(project, "legacy_table.sql", self.update_model)
        assert_success(run_dbt(["run"]))

        assert fetch_rows(
            project,
            "legacy_table",
            "ds::text, id, value",
            "ds, id",
        ) == [
            ("2024-01-02", 2, "new-2"),
            ("2024-01-02", 4, "new-4"),
        ]
        assert_logical_partitioned(project, "legacy_table")


class TestLogicalPartitionConfigMisuse:
    table_model = """
{{ config(
    materialized='table',
    incremental_partition_key='ds'
) }}
select date '2024-01-01' as ds, 1::bigint as id
"""
    incremental_model = """
{{ config(
    materialized='incremental',
    incremental_strategy='partition'
) }}
select date '2024-01-01' as ds, 1::bigint as id
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "misuse_table.sql": self.table_model,
            "misuse_incremental.sql": self.incremental_model,
        }

    @pytest.mark.parametrize("model_name", ["misuse_table", "misuse_incremental"])
    def test_old_materialization_entry_points_report_clear_error(
        self, project, model_name
    ):
        results = run_dbt(["run", "--select", model_name], expect_pass=False)

        assert len(results) == 1
        assert results[0].status == "error"
        assert (
            "require materialized='logical_partition_table'"
            in error_text(results[0])
        )
