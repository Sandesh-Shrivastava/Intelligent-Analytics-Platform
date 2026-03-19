-- stg_order_payments.sql
-- Cleans raw payments table:
--   - casts payment_value to decimal
--   - renames columns for clarity

with source as (
    select * from raw_olist.order_payments_dataset
),

cleaned as (
    select
        order_id,
        payment_sequential                       as payment_sequence,
        payment_type,
        cast(payment_installments as int)        as installments,
        cast(payment_value as decimal(10,2))     as payment_value
    from source
)

select * from cleaned