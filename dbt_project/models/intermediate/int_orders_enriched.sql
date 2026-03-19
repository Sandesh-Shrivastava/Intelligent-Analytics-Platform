-- int_orders_enriched.sql
-- Joins orders with customers and payments
-- One row per order with full context

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

payments as (
    select
        order_id,
        sum(payment_value)                  as total_payment,
        max(installments)                   as max_installments,
        count(payment_sequence)             as payment_count,
        -- most used payment type for this order
        max_by(payment_type, payment_value) as primary_payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- customer info
        c.unique_id         as customer_unique_id,
        c.city              as customer_city,
        c.state             as customer_state,

        -- payment info
        p.total_payment,
        p.max_installments,
        p.payment_count,
        p.primary_payment_type,

        -- derived metrics
        case when o.delivered_at is not null and o.purchased_at is not null
             then date_diff('day', o.purchased_at, o.delivered_at)
             else null end                                                  as delivery_days,
        case when o.delivered_at is not null and o.estimated_delivery_at is not null
             then date_diff('day', o.estimated_delivery_at, o.delivered_at)
             else null end                                                  as delivery_delay_days,
        case
            when o.delivered_at is not null and o.estimated_delivery_at is not null
             and o.delivered_at <= o.estimated_delivery_at then true
            else false
        end                                                                 as delivered_on_time

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join payments p  on o.order_id    = p.order_id
)

select * from final