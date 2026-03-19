-- stg_sellers.sql
-- Cleans raw sellers table:
--   - removes "seller_" prefix from columns

with source as (
    select * from raw_olist.sellers_dataset
),

cleaned as (
    select
        seller_id,
        seller_city     as city,
        seller_state    as state,
        seller_zip_code_prefix as zip_code
    from source
)

select * from cleaned