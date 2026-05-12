
with clean_trade as (
select
    row_number()over(order by trade_date,trade_id) as trade_sk,
    trade_id,
    CAST(TO_CHAR(CAST(trade_date AS DATE), 'YYYYMMDD') AS INT) AS trade_date,
    CAST(TO_CHAR(CAST(settlement_date AS DATE), 'YYYYMMDD') AS INT) AS settlement_date,
    instrument_id,
    trade_type,
    quantity,
    cast(execution_price as decimal(38,06)) as execution_price,
    upper(currency) as currency,
    trader_id,
    counterparty_id,
    MD5(CONCAT_WS('|', CAST(trade_id AS VARCHAR), CAST(trade_date AS VARCHAR), CAST(instrument_id AS VARCHAR))) AS dwh_message_hashcode,
    'dbt_model' as dwh_created_by,
    current_timestamp() as dwh_created_timestamp,
    CAST(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') AS INT) AS dwh_business_date
    from {{source('managed_layer','trade_load')}}
)
select 
trade_sk,
trade_id,
trade_date,
instrument_id,
trade_type,
quantity,
execution_price,
currency,
trader_id,
counterparty_id,
dwh_message_hashcode,
dwh_created_by,
dwh_created_timestamp,
dwh_business_date
from clean_trade