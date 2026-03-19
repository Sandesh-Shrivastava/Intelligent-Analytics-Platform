-- mart_customer_metrics.sql
-- ML-ready feature table for churn prediction and CLV modelling
-- One row per customer with all engineered features

with customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        customer_unique_id,

        -- ── Recency features ─────────────────────────────────────────────
        recency_days,
        recency_score,
        case when recency_days <= 90  then 1 else 0 end   as active_last_90d,
        case when recency_days <= 180 then 1 else 0 end   as active_last_180d,

        -- ── Frequency features ───────────────────────────────────────────
        total_orders,
        frequency_score,
        total_items,
        round(total_orders /
            nullif(customer_age_days, 0) * 30, 4)         as orders_per_month,

        -- ── Monetary features ────────────────────────────────────────────
        total_revenue,
        monetary_score,
        avg_order_value,
        max_order_value,

        -- ── Engagement features ──────────────────────────────────────────
        avg_review_score,
        total_reviews,
        case when total_reviews > 0
            then round(positive_reviews * 1.0
                / nullif(total_reviews, 0), 4)
            else null
        end                                                as positive_review_rate,

        -- ── Delivery features ────────────────────────────────────────────
        avg_delivery_days,
        case when total_orders > 0
            then round(on_time_deliveries * 1.0
                / nullif(total_orders, 0), 4)
            else null
        end                                                as on_time_rate,

        -- ── Location features ────────────────────────────────────────────
        customer_state,

        -- ── RFM segment ──────────────────────────────────────────────────
        customer_segment,

        -- ── Churn label (target variable for ML) ─────────────────────────
        -- Definition: customer is churned if no order in last 180 days
        case when total_orders = 1 then 1 else 0 end    as is_churned

    from customers
)

select * from final