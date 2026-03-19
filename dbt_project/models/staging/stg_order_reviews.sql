-- stg_order_reviews.sql
-- Cleans raw reviews table:
--   - casts timestamps
--   - adds is_positive flag (score >= 4)

with source as (
    select * from raw_olist.order_reviews_dataset
),

cleaned as (
    select
        review_id,
        order_id,
        cast(review_score as int)                        as score,
        review_comment_message                           as comment,
        cast(review_creation_date as timestamp)          as reviewed_at,
        cast(review_answer_timestamp as timestamp)       as answered_at,
        case when cast(review_score as int) >= 4
            then true else false end                    as is_positive
    from source
)

select * from cleaned