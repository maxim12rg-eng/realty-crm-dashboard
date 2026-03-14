

DROP TABLE IF EXISTS mart.rang_agent;

create table mart.rang_agent as with cte as (SELECT agent_name as agent, count(deal_id) as count_deal, sum(deal_amount) as sum_sale
FROM mart.deals_funnel_events
WHERE is_test = false and stage_name = 'Внесение ПВ' and deal_amount is not null and agent_name is not null and stage_date >= '2025-01-01'
group by 1)

select dense_rank() over (order by sum_sale desc) as rating, agent, sum_sale, count_deal, NOW() AS _etl_loaded_at
from cte 
order by rating;