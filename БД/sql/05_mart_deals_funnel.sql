-- ============================================================
-- MART: mart.deals_funnel_events
-- Источник: staging.b_crm_deal + staging.mops + staging.b_crm_company + staging.b_crm_contact
-- ============================================================

DROP TABLE IF EXISTS mart.deals_funnel_events;

CREATE TABLE mart.deals_funnel_events AS
SELECT 
    d.deal_id,
    d.title AS deal_title,
    'Запись на показ' AS stage_name,
    1 AS stage_order,
    d.appointed_show_dt AS stage_date,
    CASE 
        WHEN d.category_id = 10 THEN 'Прямой'
        WHEN d.category_id = 5 AND d.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department,
    d.responsible_mop_id,
    m.mop_name AS manager_name,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    d.deal_object AS object_name,
    d.area,
    d.zin_reason,
    CASE WHEN d.appointed_show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_appointed_show,
    CASE WHEN d.show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_show,
    CASE WHEN d.reserve_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_reserve,
    CASE WHEN d.deal_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_deal,
    d.deal_amount,
    d.reserve_lot_sum AS reserve_amount,
    d.is_test
FROM staging.b_crm_deal d
LEFT JOIN staging.mops m ON d.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
WHERE d.appointed_show_dt IS NOT NULL
  AND d.category_id IN (5, 10) AND d.is_test = FALSE

UNION ALL

-- Этап 2: Показ
SELECT 
    d.deal_id,
    d.title AS deal_title,
    'Показ' AS stage_name,
    2 AS stage_order,
    d.show_dt AS stage_date,
    CASE 
        WHEN d.category_id = 10 THEN 'Прямой'
        WHEN d.category_id = 5 AND d.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department,
    d.responsible_mop_id,
    m.mop_name AS manager_name,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    d.deal_object AS object_name,
    d.area,
    d.zin_reason,
    CASE WHEN d.appointed_show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_appointed_show,
    CASE WHEN d.show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_show,
    CASE WHEN d.reserve_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_reserve,
    CASE WHEN d.deal_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_deal,
    d.deal_amount,
    d.reserve_lot_sum AS reserve_amount,
    d.is_test
FROM staging.b_crm_deal d
LEFT JOIN staging.mops m ON d.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
WHERE d.show_dt IS NOT NULL
  AND d.category_id IN (5, 10) AND d.is_test = FALSE

UNION ALL

-- Этап 3: Бронь
SELECT 
    d.deal_id,
    d.title AS deal_title,
    'Бронь' AS stage_name,
    3 AS stage_order,
    d.reserve_dt AS stage_date,
    CASE 
        WHEN d.category_id = 10 THEN 'Прямой'
        WHEN d.category_id = 5 AND d.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department,
    d.responsible_mop_id,
    m.mop_name AS manager_name,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    d.deal_object AS object_name,
    d.area,
    d.zin_reason,
    CASE WHEN d.appointed_show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_appointed_show,
    CASE WHEN d.show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_show,
    CASE WHEN d.reserve_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_reserve,
    CASE WHEN d.deal_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_deal,
    d.deal_amount,
    d.reserve_lot_sum AS reserve_amount,
    d.is_test
FROM staging.b_crm_deal d
LEFT JOIN staging.mops m ON d.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
WHERE d.reserve_dt IS NOT NULL
  AND d.category_id IN (5, 10) AND d.is_test = FALSE
 
UNION ALL

-- Этап 4: Сделка
SELECT 
    d.deal_id,
    d.title AS deal_title,
    'Сделка' AS stage_name,
    4 AS stage_order,
    d.deal_dt AS stage_date,
    CASE 
        WHEN d.category_id = 10 THEN 'Прямой'
        WHEN d.category_id = 5 AND d.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department,
    d.responsible_mop_id,
    m.mop_name AS manager_name,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    d.deal_object AS object_name,
    d.area,
    d.zin_reason,
    CASE WHEN d.appointed_show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_appointed_show,
    CASE WHEN d.show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_show,
    CASE WHEN d.reserve_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_reserve,
    CASE WHEN d.deal_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_deal,
    d.deal_amount,
    d.reserve_lot_sum AS reserve_amount,
    d.is_test
FROM staging.b_crm_deal d
LEFT JOIN staging.mops m ON d.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
WHERE d.deal_dt IS NOT NULL
  AND d.category_id IN (5, 10) AND d.is_test = FALSE

UNION ALL

-- Этап 5: Внесение ПВ (из категории 4 через связь sales_deal_id)
SELECT 
    sales.deal_id,
    sales.title AS deal_title,
    'Внесение ПВ' AS stage_name,
    5 AS stage_order,
    support.pv_paid_dt AS stage_date,
    CASE 
        WHEN sales.category_id = 10 THEN 'Прямой'
        WHEN sales.category_id = 5 AND sales.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department,
    sales.responsible_mop_id,
    m.mop_name AS manager_name,
    CASE WHEN sales.uf_crm_agency ~ '^[0-9]+$' THEN sales.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN sales.uf_crm_agent ~ '^[0-9]+$' THEN sales.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    sales.deal_object AS object_name,
    sales.area,
    sales.zin_reason,
    CASE WHEN sales.appointed_show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_appointed_show,
    CASE WHEN sales.show_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_show,
    CASE WHEN sales.reserve_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_reserve,
    CASE WHEN sales.deal_dt IS NOT NULL THEN 1 ELSE 0 END AS cnt_deal,
    sales.deal_amount,
    sales.reserve_lot_sum AS reserve_amount,
    sales.is_test
FROM staging.b_crm_deal support
JOIN staging.b_crm_deal sales ON support.sales_deal_id = sales.deal_id
LEFT JOIN staging.mops m ON sales.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN sales.uf_crm_agency ~ '^[0-9]+$' THEN sales.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN sales.uf_crm_agent ~ '^[0-9]+$' THEN sales.uf_crm_agent::INTEGER END = ct.contact_id
WHERE support.category_id = 4
  AND support.pv_paid_dt IS NOT NULL
  AND sales.category_id IN (5, 10) AND sales.is_test = FALSE;

-- Проверка
SELECT stage_name, stage_order, COUNT(*) as cnt
FROM mart.deals_funnel_events
GROUP BY stage_name, stage_order
ORDER BY stage_order;
