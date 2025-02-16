select * from branches;

select *
from branches b join cities c on c.city_uuid = b.city_uuid;

select region,
    count(*)
from branches
group by region
;

create view br as
select * from branches
where region='ДальнийВостокИВосточнаяСибирь'
;

explain analyze
select *
from sales s
    inner join br b on s.branch_uuid = b.branch_uuid
order by s.num
;

explain analyze
select *
from br b
    inner join sales s on b.branch_uuid = s.branch_uuid
order by b.num
;


explain analyze
with filter_sale as (select * from sales where branch_uuid in (select branch_uuid from br))
select * from filter_sale fs
    join br on br.branch_uuid = fs.branch_uuid
order by fs.num
;

select *
from br b
    inner join sales s on b.branch_uuid = s.branch_uuid
;
