use database dbt;

use schema public;

create table orders (
order_id varchar(10),
customer_id integer, 
order_date integer,
amount decimal(10,2)
);

--top 3 customer by total specnt in each month
with cte as(
select order_id,customer_id,order_date,amount
,month(to_date(order_date::varchar, 'YYYYMMDD')) as order_month
from orders
)
select customer_id,order_month,total_spent from(
select 
customer_id,order_month,sum(amount) as total_spent
,dense_rank()over(partition by order_month order by total_spent desc) as rn
from cte
group by customer_id,order_month
)
where rn <= 3
order by order_month,total_spent desc
;
