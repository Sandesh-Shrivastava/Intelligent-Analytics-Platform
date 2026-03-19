-- int_customer_orders.sql
-- Aggregates all orders per customer
-- Builds the foundation for RFM and churn analysis

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where status = 'delivered'
),

reviews as (
    select
        order_id,
        score,
        is_positive
    from {{ ref('stg_order_reviews') }}
),

items as (
    select
        order_id,
        count(*)            as item_count,
        sum(total_amount)   as order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        o.customer_unique_id,
        o.customer_city,
        o.customer_state,

        -- order counts
        count(o.order_id)                               as total_orders,
        sum(i.item_count)                               as total_items,

        -- revenue
        sum(i.order_value)                              as total_revenue,
        avg(i.order_value)                              as avg_order_value,
        min(i.order_value)                              as min_order_value,
        max(i.order_value)                              as max_order_value,

        -- dates — used for RFM
        min(o.purchased_at)                             as first_order_at,
        max(o.purchased_at)                             as last_order_at,
        case when min(o.purchased_at) is not null and max(o.purchased_at) is not null
             then date_diff('day', min(o.purchased_at), max(o.purchased_at))
             else null end                              as customer_age_days,

        -- recency (days since last order) — key churn feature
        case when max(o.purchased_at) is not null
             then date_diff('day', max(o.purchased_at), current_timestamp)
             else null end                              as recency_days,

        -- delivery
        avg(o.delivery_days)                            as avg_delivery_days,
        sum(case when o.delivered_on_time
            then 1 else 0 end)                          as on_time_deliveries,

        -- reviews
        avg(r.score)                                    as avg_review_score,
        sum(case when r.is_positive
            then 1 else 0 end)                          as positive_reviews,
        count(r.order_id)                               as total_reviews

    from orders o
    left join items   i on o.order_id = i.order_id
    left join reviews r on o.order_id = r.order_id
    group by
        o.customer_unique_id,
        o.customer_city,
        o.customer_state
)

select * from final