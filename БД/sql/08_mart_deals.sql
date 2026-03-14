-- ============================================================
-- MART: mart.deals
-- Источник: staging.b_crm_deal + staging.mops + staging.b_crm_company + staging.b_crm_contact + raw.b_crm_field_multi
-- ============================================================

DROP TABLE IF EXISTS mart.deals;

CREATE TABLE mart.deals AS
SELECT 
    d.deal_id,
    d.deal_dt,
    d.deal_amount,
    d.responsible_mop_id AS mop_id,
    m.mop_name,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END AS agency_id,
    c.title AS agency_name,
    CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END AS agent_id,
    ct.full_name AS agent_name,
    ph.phones AS agent_phone,
    d.area,
    d.deal_object AS object_name,
    CASE 
        WHEN d.category_id = 10 THEN 'Прямой'
        WHEN d.category_id = 5 AND d.responsible_mop_id = 2544112 THEN 'Переуступки'
        ELSE 'Агентский'
    END AS sales_department
FROM staging.b_crm_deal d
LEFT JOIN staging.mops m ON d.responsible_mop_id = m.mop_id
LEFT JOIN staging.b_crm_company c ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
LEFT JOIN (
    SELECT 
        element_id,
        STRING_AGG(value, ', ') AS phones
    FROM raw.b_crm_field_multi
    WHERE UPPER(type_id) = 'PHONE' 
      AND UPPER(entity_id) = 'CONTACT'
    GROUP BY element_id
) ph ON d.uf_crm_agent = ph.element_id
WHERE d.category_id IN (5, 10)
  AND d.is_test = FALSE
  AND d.deal_dt IS NOT NULL;

-- Проверка
SELECT 
    sales_department,
    COUNT(*) AS cnt,
    COUNT(agent_phone) AS with_phone,
    SUM(deal_amount) AS total_amount
FROM mart.deals
GROUP BY sales_department
ORDER BY sales_department;
