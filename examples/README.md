# dbt-hologres Example Project

This directory contains example dbt models demonstrating the key features of the dbt-hologres adapter.

## Setup

1. Configure your `profiles.yml`:

```yaml
hologres_example:
  target: dev
  outputs:
    dev:
      type: hologres
      host: hgxxx-xx-xxx-xx-xxx.hologres.aliyuncs.com
      port: 80
      user: BASIC$dbt_user
      password: your_password
      database: from_dbt
      schema: public
      threads: 4
```

2. Run the example:

```bash
dbt debug  # Verify connection
dbt seed   # Load seed data from CSV files
dbt run    # Run all models
dbt test   # Run tests
dbt docs generate  # Generate documentation
dbt docs serve    # Serve documentation at http://localhost:8080
```

## Example Models

### 1. Simple Table Model
`models/staging/stg_orders.sql` - Basic table materialization

### 2. View Model
`models/marts/orders_summary.sql` - View materialization

### 3. Incremental Model
`models/marts/orders_incremental.sql` - Incremental updates with merge strategy

### 4. Dynamic Table Model
`models/marts/orders_dynamic_table.sql` - Hologres Dynamic Table with auto-refresh

### 5. Table with Properties
`models/marts/table_with_properties.sql` - Table with Hologres-specific properties (orientation, distribution_key, etc.)

### 6. Incremental Logical Partition Table (Single Key)
`models/marts/logical_partition_table.sql` - Partition-incremental table keyed by the DATE column `ds`

```yaml
config:
  materialized: logical_partition_table
  logical_partition_key: 'ds'
  incremental_partition_key: 'ds'
  incremental_strategy: partition
  on_schema_change: fail
```

The example uses `is_incremental()` to compare the source DATE partition key with the target's latest `ds`, including that boundary partition so its complete contents are replaced.

### 7. Incremental Logical Partition Table (Multiple Keys)
`models/marts/lpt_multi_keys.sql` - Partition-incremental table with two ordered partition columns

```yaml
config:
  materialized: logical_partition_table
  logical_partition_key: 'order_year, order_month'
  incremental_partition_key: 'order_year, order_month'
  incremental_strategy: partition
  on_schema_change: fail
```

The example compares the complete ordered `(order_year, order_month)` boundary using scalar subqueries, because Hologres does not support row-value subqueries. Comparing only `order_year` or `order_month` would select the wrong monthly boundary.

#### Partition-incremental behavior and safety

- `logical_partition_key` is required and supports 1-2 comma-separated INT, TEXT, VARCHAR, DATE, TIMESTAMP, or TIMESTAMPTZ columns. Partition columns are set to NOT NULL.
- `incremental_partition_key` is optional; when configured, it must exactly match `logical_partition_key`, including order. It enables incremental runs for an existing table and makes `is_incremental()` return true.
- `incremental_strategy` defaults to `partition` and is the only supported strategy. Do not configure `unique_key`; the strategy operates on partitions, not individual rows.
- Each incremental model result must contain every row for every partition key present in that result. The strategy deletes those complete target partitions before inserting the result, so row-level filtering would create a partial backfill.
- The first run and `--full-refresh` rebuild the table. The legacy `materialized: table` plus `logical_partition_key` mode also remains full-rebuild only; omitting `incremental_partition_key` from the dedicated materialization does the same.
- Hologres connections use autocommit with `BEGIN` disabled. `DELETE + INSERT` has no guaranteed cross-statement atomicity, so a failure between the operations may require recovery or `--full-refresh`.

## Seed Data

### Sales Targets
`seeds/salestargets.csv` - Sample sales target data for stores

Seeds are CSV files in your `seeds/` directory that dbt can load into your data warehouse:

```bash
# Load all seed files
dbt seed

# Load specific seed file
dbt seed --select salestargets

# Full refresh (drop and recreate)
dbt seed --full-refresh
```

Seeds are useful for:
- Loading reference data (e.g., country codes, status mappings)
- Loading test data for development
- Loading small lookup tables that change infrequently

**Note**: Seeds are best for small datasets (< 1MB). For larger datasets, use external data loading tools.

## Model Configurations

See `dbt_project.yml` for model-specific configurations including:
- Materialization strategies
- Dynamic Table settings
- Incremental strategies
- Schema configurations
