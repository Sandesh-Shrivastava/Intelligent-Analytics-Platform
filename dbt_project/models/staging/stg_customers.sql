-- stg_customers.sql
-- Cleans raw customers table:
--   - renames columns to remove "customer_" prefix
--   - keeps one row per customer_id

with source as (
    select * from raw_olist.customers_dataset
),

cleaned as (
    select
        customer_id,
        customer_unique_id  as unique_id,
        customer_city       as city,
        customer_state      as state,
        customer_zip_code_prefix as zip_code
    from source
)

select * from cleaned