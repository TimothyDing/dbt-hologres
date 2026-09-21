# dbt-hologres

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-hologres

`dbt-hologres` enables dbt to work with Alibaba Cloud Hologres, a real-time data warehouse compatible with PostgreSQL.

For more information on using dbt with Hologres, consult the [dbt documentation](https://docs.getdbt.com).

## Getting started

### Installation

#### Install from PyPI

```bash
pip install dbt-alibaba-cloud-hologres
```

#### Install from Source

For development or to get the latest features, you can install directly from the source code:

```bash
# Clone the repository
git clone https://github.com/aliyun/dbt-hologres.git
cd dbt-hologres

# Install in editable mode
pip install --force-reinstall -e .
```

This allows you to:

- Modify the adapter code and see changes immediately
- Contribute to the project development
- Test unreleased features

### Configuration

Configure your `profiles.yml` file:

```yaml
hologres_project:
  target: dev
  outputs:
    dev:
      type: hologres
      host: hgxxx-xx-xxx-xx-xxx.hologres.aliyuncs.com
      port: 80
      user: BASIC$your_username
      password: your_password
      database: your_database
      schema: ""  # Use empty string if no default schema needed
      threads: 4
      connect_timeout: 10
      sslmode: disable
```

## Product Features

### Core Adapter Features

| Feature | Description |
|---------|-------------|
| **HologresAdapter** | Core adapter with PostgreSQL-compatible syntax support |
| **Psycopg3 Driver** | Uses modern Psycopg 3 library for better performance |
| **Incremental Strategies** | Multiple strategies: `append`, `delete+insert`, `merge`, `microbatch` |
| **Logical Partition Incremental Refresh** | Dedicated `logical_partition_table` materialization with partition-level replacement |
| **Constraints** | Full support for `primary key`, `not null`, `unique`, `foreign key`, `check` constraints |
| **Catalog by Relation** | Enabled for better metadata management |

### Table Properties Configuration

Hologres-specific table properties can be configured in your model:

| Property | Description |
|----------|-------------|
| `orientation` | Storage direction: `column` or `row` |
| `distribution_key` | Distribution key for data sharding |
| `clustering_key` | Clustering key for query optimization |
| `event_time_column` / `segment_key` | Event time column for time-series data |
| `bitmap_columns` | Bitmap index columns |
| `dictionary_encoding_columns` | Dictionary encoding columns |

Example configuration:

```yaml
models:
  my_model:
    materialized: table
    orientation: column
    distribution_key: user_id
    clustering_key: created_at
    event_time_column: created_at
    bitmap_columns: status,type
    dictionary_encoding_columns: category
```

### Supported Data Types

The adapter supports standard PostgreSQL types plus Hologres-specific types:

| Type | Description | Example |
|------|-------------|---------|
| `roaringbitmap` | Compressed bitmap for efficient set operations | `rb_build(array[1,2,3])` |
| `json` | JSON data type (text format) | `'{"key": "value"}'::json` |
| `jsonb` | Binary JSON format with index support | `'{"key": "value"}'::jsonb` |
| `integer[]` | Integer array type | `array[1, 2, 3]` |
| `text[]` | Text array type | `array['a', 'b', 'c']` |

**RoaringBitmap Example**:

```sql
{{ config(materialized='table') }}

select
    1 as id,
    rb_build(array[1,2,3,4,5]) as user_ids  -- Creates a bitmap for user tagging
```

> **Note**: RoaringBitmap columns require `materialized='table'` for proper storage.

### Dynamic Tables

Dynamic Tables are Hologres's implementation of materialized views with automatic refresh:

```yaml
models:
  my_model:
    materialized: dynamic_table
    target_lag: "30 minutes"
    auto_refresh_enable: true
    auto_refresh_mode: auto
    computing_resource: serverless
    orientation: column
```

Supported configurations:

- `target_lag` *(required)*: Data freshness requirement (e.g., "30 minutes", "1 hours")
- `auto_refresh_enable`: Enable or disable auto-refresh (default: `true`)
- `auto_refresh_mode`: `auto`, `incremental`, or `full` (default: `auto`)
- `computing_resource`: `serverless`, `local`, or warehouse name (default: `serverless`)
- `orientation`: Storage direction: `column` or `row` (default: `column`)
- `distribution_key`: Distribution key for data sharding
- `clustering_key`: Clustering key for query optimization
- `event_time_column`: Event time column for time-series data

### Logical Partition Tables

Use the dedicated materialization to incrementally replace complete logical partitions:

```sql
{{ config(
    materialized='logical_partition_table',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    incremental_strategy='partition',
    on_schema_change='fail'
) }}
```

Supported configurations and behavior:

- `logical_partition_key` is required and accepts 1-2 comma-separated columns. Supported types are INT, TEXT, VARCHAR, DATE, TIMESTAMP, and TIMESTAMPTZ; partition keys are set to NOT NULL.
- `incremental_partition_key` enables partition-incremental runs. It is optional, but when set it must exactly match `logical_partition_key`, including column order.
- `incremental_strategy` defaults to `partition` and no other strategy is supported by this materialization. `unique_key` is not used.
- The first run and `--full-refresh` rebuild the complete table. On an incremental run, `is_incremental()` returns true, and every target partition represented in the model result is deleted and then reinserted.
- The model result must contain the complete data for every partition key it includes. Filtering to only changed rows would delete the rest of those partitions and produce partial backfills; filter on the actual partition key and include the boundary partition.
- The legacy `materialized='table'` plus `logical_partition_key` configuration remains supported, but it always performs a full table rebuild. Omitting `incremental_partition_key` from the dedicated materialization also results in a full rebuild.
- Table properties such as `orientation` and `distribution_key` remain supported.

For two-column partitions, compare the complete ordered tuple when selecting a batch—for example, `(order_year, order_month)` against the target's latest `(order_year, order_month)`—rather than filtering on only one column.

> **Non-atomic replacement:** Hologres connections use autocommit and disable `BEGIN`. The partition strategy therefore does not guarantee cross-statement atomicity for its `DELETE + INSERT`; plan recovery or a full refresh if a run fails between those operations.

### LocalDate Date Utilities

The adapter provides a powerful `LocalDate` class for date manipulation in Jinja2 templates:

#### Basic Usage

```jinja2
{%- set ds = adapter.parse_date('2024-03-15') -%}

{# Date arithmetic #}
{{ ds.sub_days(7) }}        {# 2024-03-08 #}
{{ ds.add_months(2) }}      {# 2024-05-15 #}

{# Period boundaries #}
{{ ds.start_of_month() }}   {# 2024-03-01 #}
{{ ds.end_of_quarter() }}   {# 2024-03-31 #}
```

#### Chained Operations

```jinja2
{%- set ds = adapter.parse_date('2024-03-15') -%}
{%- set start_date = ds.sub_months(2).start_of_month() -%}
{{ start_date }}  {# 2024-01-01 #}
```

#### Available Methods

| Category | Methods |
|----------|---------|
| **Arithmetic** | `add_days()`, `sub_days()`, `add_months()`, `sub_months()`, `add_years()`, `sub_years()` |
| **Period Boundaries** | `start_of_month()`, `end_of_month()`, `start_of_quarter()`, `end_of_quarter()`, `start_of_year()`, `end_of_year()`, `start_of_week()`, `end_of_week()` |
| **Formatting** | `format()`, `str()`, `to_sql()` |
| **Comparison** | `is_before()`, `is_after()`, `is_equal()`, `days_between()` |
| **Properties** | `year`, `month`, `day`, `quarter`, `day_of_week`, `day_of_year` |

#### Helper Functions

```jinja2
{# Get today's date #}
{%- set today = adapter.today() -%}

{# Parse date from string #}
{%- set ds = adapter.parse_date('2024-06-15') -%}
{%- set ds = adapter.parse_date('2024/06/15') -%}   {# Slash format #}
{%- set ds = adapter.parse_date('20240615') -%}     {# Compact format #}
```

### SQL Macros

The adapter provides various SQL macros for Hologres-specific operations:

#### CTAS with Properties

```jinja2
{%- set with_properties = [
    "orientation = 'column'",
    "distribution_key = 'id'"
] -%}
create table {{ relation }}
with (
  {{ with_properties | join(',\n  ') }}
)
as (
  {{ compiled_code }}
);
```

#### Logical Partition Table DDL

```jinja2
create table {{ relation }} (
  {{ column_definitions }}
)
logical partition by list ({{ partition_columns }});
```

#### Date Utility Macros

The following macros are available for date operations:

| Macro | Description |
|-------|-------------|
| `parse_date(date_input)` | Parse date string into LocalDate object |
| `local_date(date_input)` | Alias for `parse_date()`, providing more intuitive naming |
| `today()` | Get today's date as LocalDate |
| `ds()` | Get execution date from `EXECUTION_DATE` variable or environment |
| `format_date(local_date, format_str)` | Format LocalDate to string |
| `date_range(start_date, end_date)` | Generate list of dates between two dates (inclusive) |
| `months_between(start_date, end_date)` | Calculate approximate number of months between two dates |

**Example usage in SQL:**

```jinja2
{# Using local_date macro with to_sql() for SQL-compatible output #}
select
    {{ local_date('2024-01-15').to_sql() }} as base_date,
    {{ local_date('2024-01-15').sub_days(7).to_sql() }} as week_ago

{# Date range generation #}
{%- set start = adapter.parse_date('2024-01-01') -%}
{%- set end = adapter.parse_date('2024-01-05') -%}
{%- set days = start.days_between(end) -%}

{# Format date #}
{{ ds.format('%Y%m%d') }}
```

### Connection Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `host` | Yes | - | Hologres instance hostname |
| `port` | No | 80 | Port number |
| `user` | Yes | - | Username (case-sensitive) |
| `password` | Yes | - | Password (case-sensitive) |
| `database` | Yes | - | Database name |
| `schema` | No | "" | Default schema (use empty string "" if not needed) |
| `threads` | No | 1 | Number of threads for parallel execution |
| `connect_timeout` | No | 10 | Connection timeout in seconds |
| `sslmode` | No | disable | SSL mode (disabled by default) |
| `role` | No | - | Role to set after connecting |
| `search_path` | No | - | PostgreSQL search path to set after connecting |
| `application_name` | No | dbt_hologres_{version} | Application identifier |
| `retries` | No | 1 | Number of connection retries (quadratic backoff) |

### Testing Your Connection

Run `dbt debug` to verify your connection:

```bash
dbt debug
```

### Example Project Structure

```
my_hologres_project/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   ├── marts/
│   │   └── fct_orders.sql
│   └── schema.yml
└── tests/
    └── assert_positive_order_total.sql
```

### Important Notes

1. **Case Sensitivity**: Hologres usernames and passwords are case-sensitive
2. **Default Port**: Default port is 80 (not 5432 like PostgreSQL)
3. **SSL Mode**: SSL is disabled by default for Hologres connections
4. **Psycopg3**: This adapter uses Psycopg 3, which has API differences from Psycopg 2
5. **Model Name Restrictions**: Model names must not exceed 27 characters and are case-insensitive (converted to lowercase)
6. **Connection Retry**: Uses quadratic backoff (attempt²) for connection retries instead of exponential backoff

### Supported dbt Versions

- dbt-core >= 1.8.0
- Python >= 3.10

## Unit Tests

This project includes comprehensive unit tests with mocked database connections.

### Test Coverage Overview

| Test File | Test Classes | Test Methods | Coverage Area |
|-----------|--------------|--------------|---------------|
| test_adapter.py | 5 | 29 | Adapter core functionality and configuration |
| test_adapter_cache.py | 4 | 18 | Adapter caching mechanisms |
| test_adapter_capabilities.py | 5 | 21 | Adapter capability checks |
| test_connection.py | 15 | 70 | Connection management and credentials |
| test_relation.py | 5 | 33 | Relation objects and index management |
| test_column.py | 9 | 52 | Column handling and data types |
| test_local_date.py | 22 | 123 | LocalDate date utilities |
| test_local_date_internals.py | 2 | 19 | LocalDate internal implementation |
| test_index_config.py | 5 | 34 | Index configuration |
| test_dynamic_table_config.py | 6 | 49 | Dynamic table configuration |
| test_sql_macros.py | 6 | 26 | SQL macro rendering |
| test_logical_partition.py | 12 | 48 | Logical partition DDL, validation, materialization, and incremental refresh |
| test_date_utils_macros.py | 9 | 34 | Date utility macros |
| test_exception_handling.py | 7 | 22 | Exception handling |
| test_edge_cases.py | 7 | 62 | Edge cases and boundary conditions |
| **Total** | **119** | **640** | |

Parametrized cases expand the suite to 668 tests during pytest collection and execution.

### Test Class Descriptions

#### test_adapter.py
- `TestHologresAdapter`: Core adapter functionality, incremental strategies, timestamp operations
- `TestHologresConfig`: Table property configuration (orientation, distribution_key, etc.)
- `TestHologresAdapterParseDate`: Date parsing functionality
- `TestHologresAdapterToday`: Current date retrieval
- `TestHologresAdapterTimestampAdd`: Timestamp interval SQL generation

#### test_adapter_cache.py
- `TestAdapterCacheInvalidation`: Cache invalidation behavior
- `TestAdapterCacheConsistency`: Cache consistency checks
- `TestAdapterCachePerformance`: Cache performance characteristics
- `TestAdapterCacheEdgeCases`: Cache edge case handling

#### test_adapter_capabilities.py
- `TestAdapterCapabilities`: Adapter capability declarations
- `TestAdapterBehavior`: Adapter behavior flags
- `TestAdapterConstraints`: Constraint support capabilities
- `TestAdapterMaterializations`: Supported materializations
- `TestAdapterFeatureFlags`: Feature flag handling

#### test_connection.py
- `TestHologresCredentials`: Credential validation and defaults
- `TestHologresConnectionManager`: Connection lifecycle management
- `TestHologresConnectionManagerOpen`: Connection opening behavior
- `TestHologresCredentialsValidation`: Credential boundary validation
- `TestGetResponse`: SQL response parsing

#### test_relation.py
- `TestHologresRelation`: Relation creation and properties
- `TestGetIndexConfigChanges`: Index configuration change detection
- `TestDynamicTableConfigChanges`: Dynamic table configuration changes

#### test_local_date.py
- `TestLocalDateCreation`: Date object creation from various inputs
- `TestLocalDateSubtraction`: Date subtraction operations
- `TestLocalDateAddition`: Date addition operations
- `TestLocalDatePeriodBoundaries`: Month, quarter, year boundaries
- `TestLocalDateChaining`: Method chaining support
- `TestLocalDateProperties`: Year, month, day, quarter properties
- `TestLocalDateFormatting`: Date format string generation
- `TestLocalDateComparison`: Date comparison methods
- `TestParseDateFunction`: parse_date helper function
- `TestTodayFunction`: today() helper function
- `TestLocalDateWeekMethods`: Week boundary methods
- `TestLocalDateDayOfYear`: Day of year property

#### test_local_date_internals.py
- `TestDualAccessor`: Dual accessor implementation
- `TestMonthArithmetic`: Month calculation optimization
- `TestDateOverflow`: Date overflow handling
- `TestCallableInteger`: Callable integer subclass behavior

#### test_sql_macros.py
- `TestAdaptersMacros`: CTAS and DDL statement rendering
- `TestRelationOperations`: Drop, truncate, rename operations
- `TestSchemaOperations`: Schema creation and deletion
- `TestViewOperations`: View creation and management
- `TestInsertOperations`: Insert statement generation
- `TestTimestampOperations`: Timestamp interval operations

#### test_logical_partition.py
- `TestLogicalPartitionConfig` and `TestPartitionConfigurationMacros`: Configuration parsing and key validation
- `TestLogicalPartitionMetadata`: Existing logical-partition metadata inspection
- `TestPartitionStrategySQL`: Partition deletion and insertion SQL
- `TestLogicalPartitionMaterializationConfig`: Dedicated materialization lifecycle validation
- `TestIsIncrementalMacro`: Standard and logical-partition incremental conditions
- `TestLogicalPartitionDDL`: Single-key and two-key DDL generation

## Running Tests

### Using Hatch (Recommended)

[Hatch](https://hatch.pypa.io/) is the recommended way to run tests:

```bash
# Install hatch
pip install hatch

# Run all unit tests
hatch -e cd run unit-tests

# Run a specific test file
hatch -e cd run unit-tests tests/unit/test_adapter.py

# Run a specific test class
hatch -e cd run unit-tests tests/unit/test_adapter.py::TestHologresAdapter

# Run a specific test method
hatch -e cd run unit-tests tests/unit/test_adapter.py::TestHologresAdapter::test_date_function
```

### Using pytest Directly

You can also run tests directly with pytest:

```bash
# Install dependencies
pip install -e .
pip install pytest pytest-mock pytest-xdist freezegun

# Run all unit tests
python -m pytest tests/unit -v

# Run with parallel execution
python -m pytest tests/unit -v -n auto

# Run a specific test file
python -m pytest tests/unit/test_connection.py -v

# Run with coverage report
python -m pytest tests/unit --cov=src/dbt/adapters/hologres --cov-report=term-missing
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `unit` | Unit tests (default for tests/unit/) |
| `integration` | Integration tests (requires database connection) |
| `slow` | Slow-running tests |
| `smoke` | Basic smoke tests |

## Integration Tests

Integration tests require an actual Hologres database connection and perform real database operations including creating, updating, and dropping tables.

### Prerequisites

Before running integration tests, configure your Hologres connection using one of the following methods:

**Method 1: Using test.env file (Recommended)**

1. Copy the example environment file:

```bash
cp test.env.example test.env
```

2. Edit `test.env` and fill in your actual Hologres connection details:

```bash
# Hologres instance configuration
DBT_HOLOGRES_HOST=your_hologres_instance.hologres.aliyuncs.com
DBT_HOLOGRES_PORT=80
DBT_HOLOGRES_USER='BASIC$your_username'
DBT_HOLOGRES_PASSWORD='your_password'
DBT_HOLOGRES_DATABASE='your_database'
DBT_HOLOGRES_SCHEMA='test_schema'

# Enable integration tests
DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true
```

3. Load the environment variables before running tests:

```bash
# Load environment variables from test.env
export $(cat test.env | grep -v '^#' | xargs)

# Run integration tests
pytest tests/integration/
```

**Method 2: Setting environment variables directly**

```bash
export DBT_HOLOGRES_RUN_INTEGRATION_TESTS=true
export DBT_HOLOGRES_HOST=your_hologres_instance.hologres.aliyuncs.com
export DBT_HOLOGRES_PORT=80
export DBT_HOLOGRES_USER=your_username
export DBT_HOLOGRES_PASSWORD=your_password
export DBT_HOLOGRES_DATABASE=your_database
export DBT_HOLOGRES_SCHEMA=test_schema  # Optional, defaults to 'test_schema'
```

### Running Integration Tests

```bash
# Run all integration tests
pytest tests/integration/

# Run specific integration test
pytest tests/integration/test_table_operations.py

# Run with verbose output
pytest tests/integration/ -v

# Run only table operation tests
pytest tests/integration/test_table_operations.py -v

# Run only view operation tests
pytest tests/integration/test_view_operations.py -v

# Run only Hologres-specific feature tests
pytest tests/integration/test_hologres_features.py -v

# Run only logical-partition incremental integration tests
pytest tests/integration/test_logical_partition_incremental.py -v
```

### Integration Test Structure

The integration test suite includes:

- **test_table_operations.py**: Tests for table creation, updates, deletion, and incremental models
- **test_view_operations.py**: Tests for view creation, dependencies, and conversions
- **test_hologres_features.py**: Tests for Hologres-specific features like indexes, dynamic tables, and partitioning
- **test_logical_partition_incremental.py**: Tests single-key and two-key partition replacement against a live Hologres database

Each test uses an isolated schema to ensure tests don't interfere with each other. Test schemas are automatically cleaned up after each test run.

### Test Isolation

Integration tests create unique schemas for each test to ensure isolation:

- Each test gets a unique schema name (e.g., `test_a1b2c3d4e5f6g7h8i9j0`)
- Tests clean up their schemas automatically after completion
- Failed tests still attempt cleanup

## Functional Tests

Functional tests use the official dbt test framework (`dbt-tests-adapter`) to verify
end-to-end dbt operations against a Hologres database.

### Test Structure

```
tests/functional/
├── conftest.py              # dbt test framework configuration
├── fixtures.py              # Shared test models and seeds
├── test_basic.py            # Basic dbt run, seed, compile tests
├── test_constraints.py            # Constraint enforcement tests
├── test_custom_naming.py          # Custom schema naming tests
├── test_date_utils.py             # Date utility macro tests
├── test_error_handling.py         # Error handling and invalid input tests
├── test_logical_partition.py      # Logical partition tests
├── test_on_schema_change.py       # on_schema_change behavior tests
├── test_sql_header.py             # sql_header / GUC parameter tests
├── test_table_properties.py       # Table properties configuration tests
├── test_materializations/
│   ├── test_table.py        # Table materialization tests
│   ├── test_view.py               # View materialization tests
│   ├── test_incremental.py  # Incremental model tests
│   └── test_dynamic_table.py # Dynamic table tests
```

### Running Functional Tests

```bash
# Set required environment variables
export DBT_HOLOGRES_RUN_FUNCTIONAL_TESTS=true
export DBT_HOLOGRES_HOST=your-hologres-host
export DBT_HOLOGRES_USER=your-user
export DBT_HOLOGRES_PASSWORD=your-password
export DBT_HOLOGRES_DATABASE=your-database

# Run using Hatch
hatch -e cd run integration-tests

# Or run directly with pytest
python -m pytest tests/functional/ -v
```

### Test Coverage

| Test File | Test Classes | Coverage |
|-----------|--------------|----------|
| test_basic.py | 5 | Basic run, seed, compile, test operations |
| test_constraints.py | 8 | Not null, unique, primary key, foreign key, check, composite PK constraints |
| test_custom_naming.py | 9 | Custom schema names, model naming conventions |
| test_date_utils.py | 10 | LocalDate operations and date macros |
| test_error_handling.py | 10 | Invalid SQL, circular dependencies, invalid configs, missing refs |
| test_logical_partition.py | 12 | Logical partition creation, incremental refresh, metadata validation, and failure recovery |
| test_on_schema_change.py | 6 | Schema change behaviors: ignore, append, sync, with merge/delete+insert |
| test_sql_header.py | 7 | sql_header with table, view, incremental, dynamic table, GUC parameters |
| test_table_properties.py | 9 | Event time, segment key alias, bitmap, dictionary encoding, combined properties |
| test_materializations/test_table.py | 9 | Table materialization with indexes, properties, distribution keys |
| test_materializations/test_view.py | 6 | View creation, dependencies, aggregation, join, filter |
| test_materializations/test_incremental.py | 13 | Incremental strategies (append, merge, delete+insert, microbatch) |
| test_materializations/test_dynamic_table.py | 11 | Dynamic table creation, auto-refresh, partitions, indexes, computing resource |

## Building and Publishing

### Build

This project uses [Hatchling](https://hatch.pypa.io/) as the build backend.

```bash
# Option 1: Using hatch (if installed)
pip install hatch
hatch build

# Option 2: Using python -m build (recommended for CI)
pip install build
python -m build
```

### Verify

Validate the built package before publishing:

```bash
hatch run build:check-all

# Or using twine directly
pip install twine
twine check dist/*
```

This runs `twine check` and installation verification to ensure package quality.

### Publish to PyPI

```bash
# Install twine (if not already installed)
pip install twine

# Upload to PyPI
twine upload dist/*
```

You need to configure PyPI credentials beforehand, either via `~/.pypirc` or environment variables:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=your-pypi-api-token
```

### Version Management

The version number is maintained in a single file:

```
src/dbt/adapters/hologres/__version__.py
```

Hatchling reads the version automatically from this file during build — no need to update it elsewhere.

### Release Checklist

1. Update version in `src/dbt/adapters/hologres/__version__.py`
2. Update `CHANGELOG.md` and user-facing documentation
3. Build: `hatch build`
4. Verify: `hatch run build:check-all` or `twine check dist/*`
5. Publish: `twine upload dist/*`
6. Tag: `git tag v<version> && git push origin v<version>`

## Resources

- [dbt Documentation](https://docs.getdbt.com)
- [Hologres Documentation](https://help.aliyun.com/zh/hologres/)
- [Hologres Dynamic Table Guide](https://help.aliyun.com/zh/hologres/user-guide/introduction-to-dynamic-table)

## License

Apache License 2.0

## Support

For issues and questions:

- [GitHub Issues](https://github.com/aliyun/dbt-hologres/issues)
- [dbt Community Slack](https://www.getdbt.com/community/)
