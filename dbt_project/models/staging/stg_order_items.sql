-- stg_order_items.sql
-- Cleans raw order items table:
--   - casts price and freight to decimal
--   - casts shipping_limit_date to timestamp
--   - adds total_amount (price + freight)

with source as (
    select * from raw_olist.order_items_dataset
),

cleaned as (
    select
        order_id,
        order_item_id                                    as item_sequence,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp)           as shipping_limit_at,
        cast(price as decimal(10,2))                     as price,
        cast(freight_value as decimal(10,2))             as freight_value,
        cast(price as decimal(10,2))
            + cast(freight_value as decimal(10,2))       as total_amount
    from source
)

select * from cleaned