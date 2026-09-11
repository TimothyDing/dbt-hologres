"""Unit tests for Hologres logical partition table functionality.

These tests verify the configuration and DDL generation for logical
partition tables in Hologres.
"""
from pathlib import Path
import re
from types import SimpleNamespace
from unittest import mock

from jinja2 import Environment, FileSystemLoader, Template
from jinja2.nativetypes import NativeEnvironment
import pytest

from dbt.adapters.hologres.impl import HologresConfig


MACROS_DIR = (
    Path(__file__).resolve().parents[2] / "src" / "dbt" / "include" / "hologres" / "macros"
)


class CompilerError(Exception):
    pass


def raise_compiler_error(message):
    raise CompilerError(message)


def load_adapters_macros(config_values=None, extra_globals=None):
    environment = NativeEnvironment(
        loader=FileSystemLoader(MACROS_DIR),
        extensions=["jinja2.ext.do"],
    )
    environment.globals.update(
        {
            "adapter": SimpleNamespace(quote=lambda identifier: f'"{identifier}"'),
            "config": config_values or {},
            "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
            "get_quoted_csv": lambda columns: ", ".join(
                f'"{column}"' for column in columns
            ),
            "return": lambda value: value,
        }
    )
    environment.globals.update(extra_globals or {})
    return environment.get_template("adapters.sql").make_module()


def load_partition_metadata_macros(dump_script, is_logical_partitioned=True):
    captured_statements = {}

    def statement(name, fetch_result=False, auto_begin=True, caller=None):
        captured_statements[name] = {
            "sql": caller(),
            "fetch_result": fetch_result,
            "auto_begin": auto_begin,
        }
        return ""

    result = SimpleNamespace(
        table=SimpleNamespace(rows=[(is_logical_partitioned, dump_script)])
    )
    macros = load_adapters_macros(
        extra_globals={
            "load_result": lambda _name: result,
            "modules": SimpleNamespace(re=re),
            "statement": statement,
        }
    )
    relation = SimpleNamespace(
        schema="analytics",
        identifier="fact_orders",
        include=lambda **_kwargs: '"analytics"."fact_orders"',
    )
    return macros, relation, captured_statements


def load_materialization(materialization_name, config_values):
    materialization_path = MACROS_DIR / "materializations" / f"{materialization_name}.sql"
    source = materialization_path.read_text()
    source = re.sub(
        r"\{%[-]?\s*materialization\b.*?%\}",
        "{% macro materialization() %}",
        source,
        count=1,
    )
    source = re.sub(
        r"\{%[-]?\s*endmaterialization\s*[-]?%\}",
        "{% endmacro %}",
        source,
        count=1,
    )

    adapter_macros = load_adapters_macros(config_values)
    environment = Environment(extensions=["jinja2.ext.do"])
    environment.globals.update(
        config=config_values,
        exceptions=SimpleNamespace(raise_compiler_error=raise_compiler_error),
        hologres__parse_partition_keys=(
            adapter_macros.hologres__parse_partition_keys
        ),
        hologres__validate_incremental_partition_keys=(
            adapter_macros.hologres__validate_incremental_partition_keys
        ),
    )
    return environment.from_string(source).make_module().materialization


def load_partition_strategy_macros():
    environment = NativeEnvironment(
        loader=FileSystemLoader(MACROS_DIR),
        extensions=["jinja2.ext.do"],
    )
    environment.globals.update(
        adapter=SimpleNamespace(quote=lambda identifier: f'"{identifier}"'),
        get_quoted_csv=lambda columns: ", ".join(
            f'"{column}"' for column in columns
        ),
    )
    return environment.get_template(
        "materializations/incremental_strategies.sql"
    ).make_module()


def render_is_incremental(
    materialized,
    relation,
    config_values=None,
    *,
    execute=True,
    full_refresh=False,
    get_relation=None,
):
    adapter_macros = load_adapters_macros(config_values)
    environment = NativeEnvironment(
        loader=FileSystemLoader(MACROS_DIR),
        extensions=["jinja2.ext.do"],
    )
    relation_loader = get_relation or (lambda *_args: relation)
    environment.globals.update(
        {
            "adapter": SimpleNamespace(get_relation=relation_loader),
            "config": config_values or {},
            "execute": execute,
            "hologres__parse_partition_keys": (
                adapter_macros.hologres__parse_partition_keys
            ),
            "hologres__validate_incremental_partition_keys": (
                adapter_macros.hologres__validate_incremental_partition_keys
            ),
            "model": SimpleNamespace(
                config=SimpleNamespace(materialized=materialized)
            ),
            "return": lambda value: value,
            "should_full_refresh": lambda: full_refresh,
            "this": SimpleNamespace(
                database="db", schema="schema", table="model"
            ),
        }
    )
    rendered = environment.get_template(
        "materializations/is_incremental.sql"
    ).make_module().is_incremental()
    if isinstance(rendered, bool):
        return rendered
    return rendered.strip() == "True"


class TestLogicalPartitionConfig:
    """Test logical partition configuration in HologresConfig."""

    def test_single_partition_key_config(self):
        """Test config with single partition key."""
        config = HologresConfig(logical_partition_key="ds")
        assert config.logical_partition_key == "ds"

    def test_dual_partition_keys_config(self):
        """Test config with dual partition keys."""
        config = HologresConfig(logical_partition_key="order_year, order_month")
        assert config.logical_partition_key == "order_year, order_month"

    def test_partition_key_with_properties(self):
        """Test config with partition key and other table properties."""
        config = HologresConfig(
            orientation="column",
            distribution_key="order_id",
            logical_partition_key="ds",
        )
        assert config.orientation == "column"
        assert config.distribution_key == "order_id"
        assert config.logical_partition_key == "ds"

    def test_partition_key_none(self):
        """Test config without partition key."""
        config = HologresConfig()
        assert config.logical_partition_key is None

    def test_incremental_partition_key_config(self):
        config = HologresConfig(incremental_partition_key="ds, region")

        assert config.incremental_partition_key == "ds, region"

    def test_incremental_partition_key_defaults_to_none(self):
        assert HologresConfig().incremental_partition_key is None

    def test_partition_key_with_clustering_key(self):
        """Test config with partition key and clustering key."""
        config = HologresConfig(
            logical_partition_key="region",
            clustering_key="created_at",
        )
        assert config.logical_partition_key == "region"
        assert config.clustering_key == "created_at"


class TestPartitionConfigurationMacros:
    def test_parse_partition_keys_trims_columns(self):
        macros = load_adapters_macros()

        result = macros.hologres__parse_partition_keys(
            "  ds  ,  region  ", "logical_partition_key"
        )

        assert result == ["ds", "region"]

    def test_parse_partition_keys_allows_missing_optional_config(self):
        macros = load_adapters_macros()

        result = macros.hologres__parse_partition_keys(
            None, "incremental_partition_key"
        )

        assert result == []

    @pytest.mark.parametrize(
        "raw_value,required",
        [
            (None, True),
            ("", False),
            ("   ", False),
            ("ds,", False),
            (",ds", False),
            ("ds,,region", False),
            ("ds,ds", False),
            ("year,month,day", False),
        ],
    )
    def test_parse_partition_keys_rejects_invalid_values(self, raw_value, required):
        macros = load_adapters_macros()

        with pytest.raises(CompilerError, match="logical_partition_key"):
            macros.hologres__parse_partition_keys(
                raw_value, "logical_partition_key", required
            )

    def test_validate_partition_columns_accepts_existing_columns(self):
        macros = load_adapters_macros()
        columns = [
            SimpleNamespace(column="id"),
            SimpleNamespace(column="ds"),
        ]

        macros.hologres__validate_partition_columns(
            columns, ["ds"], "logical_partition_key"
        )

    def test_validate_partition_columns_rejects_missing_columns(self):
        macros = load_adapters_macros()
        columns = [SimpleNamespace(column="id")]

        with pytest.raises(CompilerError, match="logical_partition_key"):
            macros.hologres__validate_partition_columns(
                columns, ["ds"], "logical_partition_key"
            )

    def test_validate_incremental_partition_keys_accepts_exact_match(self):
        macros = load_adapters_macros()

        macros.hologres__validate_incremental_partition_keys(
            ["year", "month"], ["year", "month"]
        )

    @pytest.mark.parametrize(
        "incremental_columns",
        [
            ["month", "year"],
            ["year"],
            ["year", "day"],
        ],
    )
    def test_validate_incremental_partition_keys_rejects_non_exact_match(
        self, incremental_columns
    ):
        macros = load_adapters_macros()

        with pytest.raises(
            CompilerError,
            match="incremental_partition_key.*--full-refresh",
        ):
            macros.hologres__validate_incremental_partition_keys(
                ["year", "month"], incremental_columns
            )

    def test_build_table_properties_preserves_supported_configs(self):
        macros = load_adapters_macros(
            {
                "orientation": "column",
                "distribution_key": "id",
                "clustering_key": "created_at",
                "event_time_column": "event_at",
                "segment_key": "ignored_alias",
                "bitmap_columns": "status,type",
                "dictionary_encoding_columns": "region",
            }
        )

        assert macros.hologres__build_table_properties() == [
            "orientation = 'column'",
            "distribution_key = 'id'",
            "clustering_key = 'created_at'",
            "event_time_column = 'event_at'",
            "bitmap_columns = 'status,type'",
            "dictionary_encoding_columns = 'region'",
        ]

    def test_build_table_properties_uses_segment_key_alias(self):
        macros = load_adapters_macros({"segment_key": "created_at"})

        assert macros.hologres__build_table_properties() == [
            "event_time_column = 'created_at'"
        ]


class TestLogicalPartitionMetadata:
    def test_reads_ordered_partition_columns_from_official_dump_function(self):
        dump_script = """
            CREATE TABLE "analytics"."fact_orders" (...)
            LOGICAL PARTITION BY LIST ("EventDate", region);
        """
        macros, relation, statements = load_partition_metadata_macros(dump_script)
        assert hasattr(macros, "hologres__get_logical_partition_columns")

        result = macros.hologres__get_logical_partition_columns(relation)

        assert result == ["EventDate", "region"]
        statement = statements["get_logical_partition_columns"]
        normalized_sql = " ".join(statement["sql"].split())
        assert "hologres.hg_table_properties" in normalized_sql
        assert "table_namespace = 'analytics'" in normalized_sql
        assert "table_name = 'fact_orders'" in normalized_sql
        assert "property_key = 'is_logical_partitioned_table'" in normalized_sql
        assert """hg_dump_script('"analytics"."fact_orders"')""" in normalized_sql
        assert statement["fetch_result"] is True
        assert statement["auto_begin"] is False

    def test_accepts_exact_existing_logical_partition_definition(self):
        dump_script = """
            CREATE TABLE "analytics"."fact_orders" (...)
            LOGICAL PARTITION BY LIST ("ds", "region");
        """
        macros, relation, _statements = load_partition_metadata_macros(dump_script)
        assert hasattr(macros, "hologres__validate_logical_partition_definition")

        macros.hologres__validate_logical_partition_definition(
            relation, ["ds", "region"]
        )

    @pytest.mark.parametrize(
        "dump_script,is_logical_partitioned,configured_columns",
        [
            (
                "CREATE TABLE analytics.fact_orders (...) "
                "LOGICAL PARTITION BY LIST (ds);",
                False,
                ["ds"],
            ),
            (
                "CREATE TABLE analytics.fact_orders (...) "
                "LOGICAL PARTITION BY LIST (ds);",
                True,
                ["region"],
            ),
            (
                "CREATE TABLE analytics.fact_orders (...) "
                "LOGICAL PARTITION BY LIST (month, year);",
                True,
                ["year", "month"],
            ),
        ],
    )
    def test_rejects_nonmatching_existing_logical_partition_definition(
        self, dump_script, is_logical_partitioned, configured_columns
    ):
        macros, relation, _statements = load_partition_metadata_macros(
            dump_script, is_logical_partitioned
        )
        assert hasattr(macros, "hologres__validate_logical_partition_definition")

        with pytest.raises(CompilerError, match="logical partition.*--full-refresh"):
            macros.hologres__validate_logical_partition_definition(
                relation, configured_columns
            )


class TestLogicalPartitionMacroRendering:
    def test_logical_partition_ddl_quotes_all_column_identifiers(self):
        macros = load_adapters_macros()
        columns = [
            SimpleNamespace(column="Order", data_type="BIGINT"),
            SimpleNamespace(column="select", data_type="TEXT"),
            SimpleNamespace(column="ds", data_type="DATE"),
        ]

        result = macros.hologres__create_logical_partition_table_ddl(
            "analytics.fact_orders",
            columns,
            ["Order", "ds"],
            [],
        )

        assert '"Order" BIGINT not null' in result
        assert '"select" TEXT' in result
        assert '"ds" DATE not null' in result
        assert 'logical partition by list ("Order", "ds")' in result


class TestPartitionStrategySQL:
    @pytest.mark.parametrize(
        "partition_columns,expected_target,expected_source",
        [
            (
                ["ds"],
                'DBT_INTERNAL_DEST."ds"',
                'DBT_INTERNAL_SOURCE."ds"',
            ),
            (
                ["year", "month"],
                'DBT_INTERNAL_DEST."year", DBT_INTERNAL_DEST."month"',
                'DBT_INTERNAL_SOURCE."year", DBT_INTERNAL_SOURCE."month"',
            ),
        ],
    )
    def test_partition_strategy_quotes_and_qualifies_partition_keys(
        self, partition_columns, expected_target, expected_source
    ):
        macros = load_partition_strategy_macros()
        columns = [
            SimpleNamespace(name="id"),
            SimpleNamespace(name="year"),
            SimpleNamespace(name="month"),
            SimpleNamespace(name="ds"),
        ]

        result = macros.hologres__get_partition_strategy_sql(
            "analytics.target",
            "analytics.source",
            partition_columns,
            columns,
        )

        assert f"where ({expected_target}) in" in result
        assert f"select distinct {expected_source}" in result
        assert "from analytics.source as DBT_INTERNAL_SOURCE" in result

    def test_partition_strategy_uses_explicit_quoted_insert_columns(self):
        macros = load_partition_strategy_macros()
        columns = [
            SimpleNamespace(name="Order"),
            SimpleNamespace(name="select"),
            SimpleNamespace(name="ds"),
        ]

        result = macros.hologres__get_partition_strategy_sql(
            "analytics.target",
            "analytics.source",
            ["ds"],
            columns,
        )

        assert (
            'insert into analytics.target ("Order", "select", "ds")'
            in result
        )
        assert (
            'select DBT_INTERNAL_SOURCE."Order", '
            'DBT_INTERNAL_SOURCE."select", DBT_INTERNAL_SOURCE."ds"'
            in result
        )


class TestLogicalPartitionMaterializationConfig:
    def test_requires_logical_partition_key_before_relation_operations(self):
        materialization = load_materialization(
            "logical_partition_table",
            {"incremental_strategy": "partition"},
        )

        with pytest.raises(CompilerError, match="logical_partition_key is required"):
            materialization()

    def test_rejects_non_partition_strategy_before_relation_operations(self):
        materialization = load_materialization(
            "logical_partition_table",
            {
                "logical_partition_key": "ds",
                "incremental_strategy": "append",
            },
        )

        with pytest.raises(CompilerError, match="only supports.*partition"):
            materialization()

    def test_rejects_mismatched_incremental_partition_key(self):
        materialization = load_materialization(
            "logical_partition_table",
            {
                "logical_partition_key": "year, month",
                "incremental_partition_key": "month, year",
            },
        )

        with pytest.raises(CompilerError, match="incremental_partition_key"):
            materialization()

    def test_lifecycle_avoids_guarded_ctas_and_unsupported_end(self):
        materialization_path = (
            MACROS_DIR / "materializations" / "logical_partition_table.sql"
        )
        source = materialization_path.read_text()
        source_without_comments = re.sub(
            r"\{#.*?#\}", "", source, flags=re.DOTALL
        )

        assert "get_create_table_as_sql" not in source_without_comments
        assert "end;" not in source_without_comments
        incremental_branch = source[
            source.index("{% if is_partition_incremental %}"):
            source.index("{% else %}", source.index("{% if is_partition_incremental %}"))
        ]
        source_columns_index = incremental_branch.index(
            "adapter.get_columns_in_relation(temp_relation)"
        )
        source_validation_index = incremental_branch.index(
            "source_columns, incremental_partition_columns"
        )
        definition_validation_index = incremental_branch.index(
            "hologres__validate_logical_partition_definition"
        )
        expand_types_index = incremental_branch.index(
            "adapter.expand_target_column_types"
        )
        schema_change_index = incremental_branch.index("process_schema_changes")
        target_columns_index = incremental_branch.index(
            "adapter.get_columns_in_relation(target_relation)"
        )
        target_validation_index = incremental_branch.index(
            "target_columns, logical_partition_columns"
        )
        dest_validation_index = incremental_branch.index(
            "dest_columns, incremental_partition_columns"
        )

        assert source_columns_index < source_validation_index
        assert source_validation_index < definition_validation_index
        assert definition_validation_index < expand_types_index
        assert expand_types_index < schema_change_index
        assert schema_change_index < target_columns_index
        assert target_columns_index < target_validation_index
        assert target_validation_index < dest_validation_index
        assert "hologres__validate_partition_columns" in source
        assert "adapter.rename_relation(existing_relation, backup_relation)" in source
        assert "adapter.rename_relation(intermediate_relation, target_relation)" in source
        assert "create_indexes(intermediate_relation)" in source
        assert "config.get('sql_header', none)" in source


class TestIsIncrementalMacro:
    @pytest.mark.parametrize(
        "materialized,relation,config_values,full_refresh,expected",
        [
            ("incremental", SimpleNamespace(type="table"), {}, False, True),
            ("incremental", SimpleNamespace(type="view"), {}, False, False),
            ("incremental", SimpleNamespace(type="table"), {}, True, False),
            (
                "logical_partition_table",
                SimpleNamespace(type="table"),
                {
                    "logical_partition_key": "ds",
                    "incremental_partition_key": "ds",
                },
                False,
                True,
            ),
            (
                "logical_partition_table",
                SimpleNamespace(type="table"),
                {
                    "logical_partition_key": "ds",
                    "incremental_partition_key": "ds",
                    "incremental_strategy": "append",
                },
                False,
                False,
            ),
            (
                "logical_partition_table",
                SimpleNamespace(type="table"),
                {"logical_partition_key": "ds"},
                False,
                False,
            ),
            (
                "logical_partition_table",
                SimpleNamespace(type="view"),
                {
                    "logical_partition_key": "ds",
                    "incremental_partition_key": "ds",
                },
                False,
                False,
            ),
            (
                "logical_partition_table",
                SimpleNamespace(type="table"),
                {
                    "logical_partition_key": "ds",
                    "incremental_partition_key": "ds",
                },
                True,
                False,
            ),
            (
                "logical_partition_table",
                None,
                {
                    "logical_partition_key": "ds",
                    "incremental_partition_key": "ds",
                },
                False,
                False,
            ),
        ],
    )
    def test_incremental_conditions(
        self,
        materialized,
        relation,
        config_values,
        full_refresh,
        expected,
    ):
        assert render_is_incremental(
            materialized,
            relation,
            config_values,
            full_refresh=full_refresh,
        ) is expected

    def test_execute_false_skips_relation_lookup(self):
        def fail_relation_lookup(*_args):
            raise AssertionError("adapter.get_relation must not be called")

        assert render_is_incremental(
            "logical_partition_table",
            None,
            {
                "logical_partition_key": "ds",
                "incremental_partition_key": "ds",
            },
            execute=False,
            get_relation=fail_relation_lookup,
        ) is False

    def test_rejects_invalid_logical_partition_incremental_config(self):
        with pytest.raises(CompilerError, match="incremental_partition_key"):
            render_is_incremental(
                "logical_partition_table",
                SimpleNamespace(type="table"),
                {
                    "logical_partition_key": "year, month",
                    "incremental_partition_key": "month, year",
                },
            )


class TestPartitionConfigMisuse:
    @pytest.mark.parametrize("materialization_name", ["table", "incremental"])
    @pytest.mark.parametrize(
        "config_values",
        [
            {"incremental_partition_key": "ds"},
            {"incremental_partition_key": ""},
            {"incremental_strategy": "partition"},
        ],
    )
    def test_rejects_partition_incremental_config_before_relation_operations(
        self, materialization_name, config_values
    ):
        materialization = load_materialization(materialization_name, config_values)

        with pytest.raises(CompilerError, match="logical_partition_table"):
            materialization()


class TestLogicalPartitionDDL:
    """Test DDL generation for logical partition tables."""

    def test_single_key_ddl_generation(self, sample_columns):
        """Test DDL generation with single partition key."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }});
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        # Verify partition column has NOT NULL
        assert "ds TEXT not null" in result
        # Verify non-partition columns don't have NOT NULL
        assert "id BIGINT" in result
        assert "name TEXT" in result
        # Verify partition clause
        assert "logical partition by list (ds)" in result

    def test_dual_keys_ddl_generation(self):
        """Test DDL generation with dual partition keys."""
        # Create columns including partition keys
        columns = []
        for name, dtype in [
            ("id", "BIGINT"),
            ("order_year", "INT"),
            ("order_month", "INT"),
        ]:
            col = mock.MagicMock()
            col.column = name
            col.data_type = dtype
            columns.append(col)

        template_str = """
{%- set logical_partition_key = "order_year, order_month" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }});
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=columns
        ).strip()

        # Verify both partition columns have NOT NULL
        assert "order_year INT not null" in result
        assert "order_month INT not null" in result
        # Verify partition clause with both keys
        assert "logical partition by list (order_year, order_month)" in result

    def test_partition_column_not_null_constraint(self, sample_columns):
        """Test that partition columns get NOT NULL constraint."""
        template_str = """
{%- set partition_columns = ["ds"] -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
{{ col_defs | join(', ') }}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        # Verify partition column has NOT NULL
        assert "ds TEXT not null" in result
        # Verify non-partition columns don't have NOT NULL added
        assert "id BIGINT not null" not in result

    def test_ddl_with_with_clause(self, sample_columns):
        """Test DDL generation with WITH clause properties."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
{%- set with_properties = ["orientation = 'column'", "distribution_key = 'id'"] -%}
create table test_table (
  {{ col_defs | join(', ') }}
)
logical partition by list ({{ partition_columns | join(', ') }})
with (
  {{ with_properties | join(',\\n  ') }}
);
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        assert "ds TEXT not null" in result
        assert "logical partition by list (ds)" in result
        assert "with (" in result
        assert "orientation = 'column'" in result
        assert "distribution_key = 'id'" in result


class TestLogicalPartitionEdgeCases:
    """Test edge cases for logical partition configuration."""

    def test_empty_partition_key_error(self):
        """Test that empty partition key is handled."""
        config = HologresConfig(logical_partition_key="")
        assert config.logical_partition_key == ""

    def test_partition_key_with_spaces(self):
        """Test partition key with extra spaces is preserved."""
        config = HologresConfig(logical_partition_key=" year , month ")
        # The config stores the value as-is
        assert config.logical_partition_key == " year , month "

    def test_partition_key_with_trailing_comma(self):
        """Test partition key with trailing comma."""
        config = HologresConfig(logical_partition_key="ds,")
        assert config.logical_partition_key == "ds,"

    def test_partition_key_normalization_in_template(self):
        """Test that partition key is normalized in template rendering."""
        # Template should handle trimming spaces
        template_str = """
{%- set logical_partition_key = "  ds  ,  region  " -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{{ partition_columns | join(', ') }}
"""
        result = Template(template_str).render().strip()
        assert result == "ds, region"

    def test_three_partition_keys_in_template(self):
        """Test template behavior with three partition keys (Hologres supports 1-2)."""
        # The template logic should still work, but Hologres may reject it
        template_str = """
{%- set logical_partition_key = "year, month, day" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{{ partition_columns | join(', ') }}
"""
        result = Template(template_str).render().strip()
        assert result == "year, month, day"


class TestLogicalPartitionMacro:
    """Test the hologres__create_logical_partition_table_ddl macro logic."""

    def test_build_column_definitions_macro(self, sample_columns):
        """Test build_column_definitions macro logic."""
        template_str = """
{%- set partition_columns = ["ds"] -%}
{%- set col_defs = [] -%}
{%- for col in columns -%}
  {%- set col_name = col.column -%}
  {%- set col_type = col.data_type -%}
  {%- if col_name in partition_columns -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type ~ ' not null') -%}
  {%- else -%}
    {%- do col_defs.append(col_name ~ ' ' ~ col_type) -%}
  {%- endif -%}
{%- endfor -%}
{{ col_defs | join(', ') }}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            columns=sample_columns
        ).strip()

        # Verify correct column definitions
        assert "id BIGINT" in result
        assert "name TEXT" in result
        assert "ds TEXT not null" in result
        assert "created_at TIMESTAMP" in result

    def test_partition_key_parsing_single(self):
        """Test partition key parsing for single key."""
        template_str = """
{%- set logical_partition_key = "ds" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- for col in partition_columns -%}
{{ col }}
{%- if not loop.last %}, {% endif -%}
{%- endfor -%}
"""
        result = Template(template_str).render().strip()
        assert result == "ds"

    def test_partition_key_parsing_multiple(self):
        """Test partition key parsing for multiple keys."""
        template_str = """
{%- set logical_partition_key = "year, month" -%}
{%- set partition_columns = logical_partition_key.split(',') | map('trim') | list -%}
{%- for col in partition_columns -%}
{{ col }}
{%- if not loop.last %}, {% endif -%}
{%- endfor -%}
"""
        result = Template(template_str).render().strip()
        assert result == "year, month"

    def test_with_properties_list_generation(self):
        """Test WITH properties list generation."""
        template_str = """
{%- set with_properties = [] -%}
{%- if orientation is not none -%}
  {%- do with_properties.append("orientation = '" ~ orientation ~ "'") -%}
{%- endif -%}
{%- if distribution_key is not none -%}
  {%- do with_properties.append("distribution_key = '" ~ distribution_key ~ "'") -%}
{%- endif -%}
{%- if with_properties | length > 0 -%}
with (
  {{ with_properties | join(',\\n  ') }}
)
{%- endif -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            orientation="column",
            distribution_key="id"
        ).strip()
        assert "orientation = 'column'" in result
        assert "distribution_key = 'id'" in result

    def test_with_properties_empty(self):
        """Test WITH clause is omitted when no properties."""
        template_str = """
{%- set with_properties = [] -%}
{%- if orientation is not none -%}
  {%- do with_properties.append("orientation = '" ~ orientation ~ "'") -%}
{%- endif -%}
{%- if with_properties | length > 0 -%}
with (
  {{ with_properties | join(',\\n  ') }}
)
{%- endif -%}
"""
        result = Template(template_str, extensions=["jinja2.ext.do"]).render(
            orientation=None
        ).strip()
        assert "with (" not in result


class TestLogicalPartitionIntegration:
    """Test logical partition integration with other features."""

    def test_logical_partition_with_index_config(self):
        """Test logical partition can be used with index config."""
        from dbt.adapters.hologres.relation_configs import HologresIndexConfig

        config = HologresConfig(
            logical_partition_key="ds",
            indexes=[HologresIndexConfig(columns=["id"], unique=True)]
        )
        assert config.logical_partition_key == "ds"
        assert len(config.indexes) == 1
        assert config.indexes[0].unique is True

    def test_logical_partition_with_event_time_column(self):
        """Test logical partition with event_time_column (both time-related features)."""
        config = HologresConfig(
            logical_partition_key="ds",
            event_time_column="created_at",
        )
        assert config.logical_partition_key == "ds"
        assert config.event_time_column == "created_at"

    def test_logical_partition_with_segment_key(self):
        """Test logical partition with segment_key alias."""
        config = HologresConfig(
            logical_partition_key="region",
            segment_key="updated_at",
        )
        assert config.logical_partition_key == "region"
        # Note: segment_key is stored but event_time_column is not automatically set
        assert config.segment_key == "updated_at"