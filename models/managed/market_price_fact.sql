with clean_price as(
    select
    row_number()over(order by market_date) as market_price_sk,
    CAST(TO_CHAR(CAST(market_date AS DATE), 'YYYYMMDD') AS INT) AS market_date,
    instrument_id,
    close_price,
    MD5(CONCAT_WS('|', CAST(market_date AS VARCHAR), CAST(instrument_id AS VARCHAR))) AS dwh_message_hashcode,
    'dbt_model' as dwh_created_by,
    current_timestamp() as dwh_created_timestamp,
    CAST(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') AS INT) AS dwh_business_date
    from {{source('managed_layer','market_price_load')}}
)
select 
    market_price_sk,
    market_date,
    instrument_id,
    close_price,
    dwh_message_hashcode,
    dwh_created_by,
    dwh_created_timestamp,
    dwh_business_date
from clean_price