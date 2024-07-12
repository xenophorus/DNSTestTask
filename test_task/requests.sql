select *
from (
    select
        row_number() over (partition by branch_uuid order by branch_uuid, sum(quantity) desc) as num,
        branch_uuid,
        product_uuid,
        sum(quantity) as total_quantity,
        sum(sale) as total_sales
    from sales
    group by branch_uuid, product_uuid
    order by branch_uuid , total_quantity desc ) branch_sales
where num <= 10;


select *
from (
    select
        row_number() over (partition by b.city_uuid order by b.city_uuid, sum(quantity) desc) as num,
        b.city_uuid,
        product_uuid,
        sum(quantity) as total_quantity,
        sum(sale) as total_sales
    from sales s join branches b on s.branch_uuid = b.branch_uuid
    group by b.city_uuid, s.product_uuid
    order by b.city_uuid, total_quantity desc) city_sales
where num <= 10;



select branch_uuid,
       sum(quantity) as total_quantity,
       round(sum(sale::numeric), 2) as total_sales
from sales
group by branch_uuid
order by total_quantity desc
limit 10;



with cte as (
    select
        date_time::date as date_,
        product_uuid,
        sum(quantity) as product_sum
    from sales
    group by date_, product_uuid
    order by product_uuid)
select
    cte.product_uuid,
    sum(product_sum) as total_products_sold,
    avg(product_sum) as avg_products_per_day,
    p.product_name
from cte
    join products p on p.product_uuid = cte.product_uuid
group by cte.product_uuid, p.product_name
order by total_products_sold desc;


with cte as (
    select
        date_part('month', s.date_time) as month,
        s.branch_uuid,
        sum(s.quantity) as quantity_by_month,
        sum(s.sale) as sale_by_month
    from sales s
        join branches b on b.branch_uuid = s.branch_uuid
        join cities c on c.city_uuid = b.city_uuid
    where c.city_name = 'Екатеринбург'
    group by month, s.branch_uuid)
select
    branch_uuid,
    month,
    quantity_by_month,
    sale_by_month,
    sum(quantity_by_month) over(partition by branch_uuid) as total_sum
from cte
order by month, total_sum desc
limit 2;


select
    distinct
    date_part('dow', date_time) as day_of_week,
    date_part('hour', date_time) as hour,
    sum(quantity) over (partition by date_part('dow', date_time)) as total_by_weekday,
    sum(quantity) over (partition by date_part('hour', date_time)) as total_by_hour
from sales
order by total_by_weekday, total_by_hour desc
limit 1;
