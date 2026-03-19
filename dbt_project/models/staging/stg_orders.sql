-- stg_orders.sql
with source as (
    select * from {{ source('raw_olist', 'orders_dataset') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        order_status                                                         as status,
        cast(nullif(order_purchase_timestamp, '')      as timestamp)        as purchased_at,
        cast(nullif(order_approved_at, '')             as timestamp)        as approved_at,
        cast(nullif(order_delivered_carrier_date, '')  as timestamp)        as shipped_at,
        cast(nullif(order_delivered_customer_date, '') as timestamp)        as delivered_at,
        cast(nullif(order_estimated_delivery_date, '') as timestamp)        as estimated_delivery_at
    from source
)

select * from cleaned