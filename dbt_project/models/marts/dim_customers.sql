-- dim_customers.sql
-- Customer dimension table
-- One row per unique customer with full profile

with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

final as (
    select
        customer_unique_id,
        customer_city,
        customer_state,

        -- order behaviour
        total_orders,
        total_items,
        total_revenue,
        avg_order_value,
        min_order_value,
        max_order_value,

        -- dates
        first_order_at,
        last_order_at,
        customer_age_days,
        recency_days,

        -- delivery
        avg_delivery_days,
        on_time_deliveries,

        -- reviews
        avg_review_score,
        positive_reviews,
        total_reviews,

        -- RFM scores (1-5 scale, 5 = best)
        ntile(5) over (order by recency_days desc)      as recency_score,
        ntile(5) over (order by total_orders asc)       as frequency_score,
        ntile(5) over (order by total_revenue asc)      as monetary_score,

        -- customer segment based on RFM
        case
            when ntile(5) over (order by recency_days desc)  >= 4
            and ntile(5) over (order by total_orders asc)   >= 4
            then 'Champion'
            when ntile(5) over (order by recency_days desc)  >= 3
            and ntile(5) over (order by total_orders asc)   >= 3
            then 'Loyal'
            when ntile(5) over (order by recency_days desc)  >= 4
            and ntile(5) over (order by total_orders asc)   <= 2
            then 'New Customer'
            when ntile(5) over (order by recency_days desc)  <= 2
            and ntile(5) over (order by total_orders asc)   >= 3
            then 'At Risk'
            when ntile(5) over (order by recency_days desc)  <= 2
            and ntile(5) over (order by total_orders asc)   <= 2
            then 'Lost'
            else 'Potential'
        end                                             as customer_segment

    from customer_orders
)

select * from final