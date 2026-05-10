{{config (materialized = 'table')}}

with testdata as(
    select 1 as id
)
select id from testdata