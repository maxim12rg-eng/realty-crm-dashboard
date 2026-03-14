
drop table if exists mart.rang_an;

create table mart.rang_an as with cte as (SELECT agency_name as an, count(deal_id) as count_deal, sum(deal_amount) as sum_sale
FROM mart.deals_funnel_events
WHERE is_test = false and stage_name = 'Внесение ПВ' and deal_amount is not null and agency_name is not null and stage_date >= '2025-01-01'
group by 1)

select dense_rank() over (order by sum_sale desc) as rating, an, sum_sale, count_deal, NOW() AS _etl_loaded_at
from cte 
order by rating;
