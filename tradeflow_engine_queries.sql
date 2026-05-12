CREATE OR REPLACE STORAGE INTEGRATION TRADEFLOWENGINE_AWS_STORAGE_INTEGRATION
TYPE = EXTERNAL_STAGE
ENABLED=TRUE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::796827774440:role/Pranjali'
STORAGE_ALLOWED_LOCATIONS =('s3://tradeflow-engine-raw/');

show storage integrations;

describe integration TRADEFLOWENGINE_AWS_STORAGE_INTEGRATION;

--Handshake between snowflake and S3 is done

use database dbt;

create or replace schema raw;

use schema raw;

Create or Replace file format CSVTYPE
TYPE='CSV'
SKIP_HEADER=1
FIELD_DELIMITER=','
RECORD_DELIMITER='\n'
FIELD_OPTIONALLY_ENCLOSED_BY ='"'

CREATE OR REPLACE STAGE TRADEFLOWENGINE_SOURCE_AWS_STAGE
FILE_FORMAT='CSVTYPE'
STORAGE_INTEGRATION = TRADEFLOWENGINE_AWS_STORAGE_INTEGRATION
URL='s3://tradeflow-engine-raw/trade/'; 

CREATE OR REPLACE PIPE trade_snowpipe 
  AUTO_INGEST = TRUE 
AS
  COPY INTO dbt.raw.trade_load
  FROM @TRADEFLOWENGINE_SOURCE_AWS_STAGE
  ON_ERROR = 'CONTINUE';

  CREATE OR REPLACE TABLE dbt.raw.trade_load (
    trade_id VARCHAR,
    trade_date DATE,
    settlement_date DATE,
    instrument_id VARCHAR,
    trade_type VARCHAR,
    quantity NUMBER,
    execution_price NUMBER(18,4),
    currency VARCHAR,
    trader_id VARCHAR,
    counterparty_id VARCHAR
);

list @TRADEFLOWENGINE_SOURCE_AWS_STAGE;

describe pipe  trade_snowpipe ;

SELECT SYSTEM$PIPE_STATUS('trade_snowpipe');

select *--CAST(TO_CHAR(CAST(trade_date AS DATE), 'YYYYMMDD') AS INT) AS trade_date_key 
from dbt.raw.trade_load;

describe table dbt.raw.trade_load;

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
    from dbt.raw.trade_load
)
select * from clean_trade;

use schema managed;

show tables;

select * from trade_fact;




