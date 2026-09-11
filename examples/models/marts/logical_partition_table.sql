-- Example: Incremental Logical Partition Table (Single Partition Key)
-- Each incremental batch reprocesses the complete latest date partition and any newer ones.

{{ config(
    materialized='logical_partition_table',
    schema='marts',
    orientation='column',
    distribution_key='order_id',
    logical_partition_key='ds',
    incremental_partition_key='ds',
    incremental_strategy='partition',
    on_schema_change='fail'
) }}

-- The incremental key must exactly match the logical key. The partition strategy
-- deletes every target partition present in this result before inserting the result,
-- so this query must return all rows for every selected ds value.

with orders as (
    select
        order_id,
        customer_id,
        amount,
        cast(order_date as date) as ds
    from {{ ref('stg_orders') }}
),

partition_batch as (
    select *
    from orders

    {% if is_incremental() %}
    -- Compare DATE to DATE and include the latest partition in full.
    where not exists (select 1 from {{ this }})
       or ds >= (select max(ds) from {{ this }})
    {% endif %}
)

select
    order_id,
    customer_id,
    ds,
    cast(sum(amount) as numeric(12,2)) as total_amount,
    count(*) as item_count
from partition_batch
group by
    order_id,
    customer_id,
    ds
