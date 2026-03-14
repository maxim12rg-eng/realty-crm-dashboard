-- ============================================================
-- BOT: bot.deal_facts
-- Источник: staging.b_crm_deal + staging.b_crm_company + staging.b_crm_contact
-- Режим: UPSERT (сохраняем историю)
-- ============================================================
CREATE TABLE IF NOT EXISTS bot.deal_facts (
    deal_id BIGINT PRIMARY KEY,
    category_id INTEGER,
    responsible_mop_id INTEGER,
    show_dt DATE,
    reserve_dt DATE,
    deal_dt DATE,
    appointed_show_dt DATE,
    pv_paid_dt DATE,
    deal_amount NUMERIC,
    reserve_lot_sum NUMERIC,
    deal_object TEXT,
    area NUMERIC,
    agency_id INTEGER,
    agency TEXT,
    agent TEXT,
    _etl_loaded_at TIMESTAMP
);
-- Шаг 1: Вставка/обновление сделок из категорий 5, 10
INSERT INTO bot.deal_facts (
    deal_id, category_id, responsible_mop_id,
    show_dt, reserve_dt, deal_dt, appointed_show_dt, pv_paid_dt,
    deal_amount, reserve_lot_sum, deal_object, area, agency_id,
    _etl_loaded_at
)
SELECT 
    d.deal_id,
    d.category_id,
    d.responsible_mop_id,
    d.show_dt,
    d.reserve_dt,
    d.deal_dt,
    d.appointed_show_dt,
    NULL as pv_paid_dt,
    d.deal_amount,
    d.reserve_lot_sum,
    d.deal_object,
    d.area,
    CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END,
    NOW() AS _etl_loaded_at
FROM staging.b_crm_deal d
WHERE d.category_id IN (5, 10)
  AND d.is_test = FALSE
ON CONFLICT (deal_id) DO UPDATE SET
    category_id = EXCLUDED.category_id,
    responsible_mop_id = EXCLUDED.responsible_mop_id,
    show_dt = EXCLUDED.show_dt,
    reserve_dt = EXCLUDED.reserve_dt,
    deal_dt = EXCLUDED.deal_dt,
    appointed_show_dt = EXCLUDED.appointed_show_dt,
    deal_amount = EXCLUDED.deal_amount,
    reserve_lot_sum = EXCLUDED.reserve_lot_sum,
    deal_object = EXCLUDED.deal_object,
    area = EXCLUDED.area,
    agency_id = EXCLUDED.agency_id,
    _etl_loaded_at = NOW();

-- Шаг 2: Обновляем pv_paid_dt из категории 4 через связь sales_deal_id
UPDATE bot.deal_facts bf
SET pv_paid_dt = s.pv_paid_dt
FROM staging.b_crm_deal s
WHERE s.category_id = 4
  AND s.sales_deal_id = bf.deal_id
  AND s.pv_paid_dt IS NOT NULL;

-- Шаг 3: Обновляем agency (название компании) и agent (ФИО контакта)
UPDATE bot.deal_facts bf
SET 
    agency = c.title,
    agent = ct.full_name
FROM staging.b_crm_deal d
LEFT JOIN staging.b_crm_company c 
    ON CASE WHEN d.uf_crm_agency ~ '^[0-9]+$' THEN d.uf_crm_agency::INTEGER END = c.company_id
LEFT JOIN staging.b_crm_contact ct 
    ON CASE WHEN d.uf_crm_agent ~ '^[0-9]+$' THEN d.uf_crm_agent::INTEGER END = ct.contact_id
WHERE bf.deal_id = d.deal_id;

-- Проверка
SELECT 
    COUNT(*) as total,
    COUNT(pv_paid_dt) as with_pv_date,
    COUNT(agency) as with_agency,
    COUNT(agent) as with_agent,
    SUM(deal_amount) FILTER (WHERE pv_paid_dt >= '2025-12-01') as pv_sum_december
FROM bot.deal_facts;
