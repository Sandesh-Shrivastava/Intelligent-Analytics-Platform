-- fct_orders.sql
-- Final orders fact table
-- One row per order — used for dashboards and reporting

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

items as (
    select
        order_id,
        count(*)          as item_count,
        sum(total_amount) as order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

reviews as (
    select
        order_id,
        score       as review_score,
        is_positive as is_positive_review
    from {{ ref('stg_order_reviews') }}
),

final as (
    select
        -- keys
        o.order_id,
        o.customer_id,
        o.customer_unique_id,

        -- order details
        o.status,
        o.primary_payment_type,
        o.max_installments,

        -- amounts
        i.item_count,
        i.order_value,
        o.total_payment,

        -- dates
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- date parts — useful for time-based analysis
        year(o.purchased_at)                    as purchase_year,
        month(o.purchased_at)                   as purchase_month,
        day_of_week(o.purchased_at)             as purchase_dow,

        -- delivery metrics
        o.delivery_days,
        o.delivery_delay_days,
        o.delivered_on_time,

        -- customer location
        o.customer_city,
        o.customer_state,

        -- review
        r.review_score,
        r.is_positive_review

    from orders o
    left join items   i on o.order_id = i.order_id
    left join reviews r on o.order_id = r.order_id
)

select * from final