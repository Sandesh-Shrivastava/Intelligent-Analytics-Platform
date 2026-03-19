-- int_order_items_enriched.sql
-- Joins order items with product and seller info
-- One row per order item with full product context

with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        i.order_id,
        i.item_sequence,
        i.product_id,
        i.seller_id,
        i.price,
        i.freight_value,
        i.total_amount,
        i.shipping_limit_at,

        -- product info
        p.category              as product_category,
        p.weight_g              as product_weight_g,
        p.photos_qty            as product_photos_qty,

        -- seller info
        s.city                  as seller_city,
        s.state                 as seller_state,

        -- derived
        round(i.freight_value
            / nullif(i.total_amount, 0) * 100, 2)  as freight_pct

    from items i
    left join products p on i.product_id = p.product_id
    left join sellers  s on i.seller_id  = s.seller_id
)

select * from final