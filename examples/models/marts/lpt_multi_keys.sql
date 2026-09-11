-- Example: Incremental Logical Partition Table with Two Partition Keys
-- The ordered (order_year, order_month) tuple identifies each monthly partition.
-- Note: Model name shortened to avoid the PostgreSQL identifier length limit.

{{ config(
    materialized='logical_partition_table',
    schema='marts',
    orientation='column',
    distribution_key='order_id',
    logical_partition_key='order_year, order_month',
    incremental_partition_key='order_year, order_month',
    incremental_strategy='partition',
    on_schema_change='fail'
) }}

-- Both key lists must match exactly, including order. The partition strategy deletes
-- every target partition present in this result, so each selected month must be complete.

with orders as (
    select
        order_id,
        customer_id,
        amount,
        extract(year from order_date)::int as order_year,
        extract(month from order_date)::int as order_month
    from {{ ref('stg_orders') }}
),

partition_batch as (
    select *
    from orders

    {% if is_incremental() %}
    -- Compare both keys in scalar subqueries; Hologres does not support
    -- row-value subqueries. This is equivalent to ordered tuple comparison.
    where not exists (select 1 from {{ this }})
       or order_year > (select max(order_year) from {{ this }})
       or (
            order_year = (select max(order_year) from {{ this }})
            and order_month >= (
                select max(order_month)
                from {{ this }}
                where order_year = (select max(order_year) from {{ this }})
            )
       )
    {% endif %}
)

select
    order_id,
    customer_id,
    order_year,
    order_month,
    cast(sum(amount) as numeric(12,2)) as total_amount,
    count(*) as item_count
from partition_batch
group by
    order_id,
    customer_id,
    order_year,
    order_month
