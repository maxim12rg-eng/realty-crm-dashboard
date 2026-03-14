-- ============================================================
-- STAGING: staging.b_crm_deal
-- Источник: raw.b_crm_deal + raw.b_uts_crm_deal + raw.b_user_field_enum
-- ============================================================

DROP TABLE IF EXISTS staging.b_crm_deal;

CREATE TABLE staging.b_crm_deal AS
SELECT 
    -- Идентификаторы
    NULLIF(TRIM(d.id), '')::BIGINT AS deal_id,
    TRIM(d.title) AS title,
    NULLIF(TRIM(d.category_id), '')::INTEGER AS category_id,
    TRIM(d.stage_id) AS stage_id,
    NULLIF(TRIM(d.assigned_by_id), '')::INTEGER AS assigned_by_id,
    NULLIF(REGEXP_REPLACE(d.company_id, '\.0+$', ''), '')::INTEGER AS company_id,
    NULLIF(REGEXP_REPLACE(d.contact_id, '\.0+$', ''), '')::INTEGER AS contact_id,
    NULLIF(REGEXP_REPLACE(u.uf_crm_responsible_mop, '\.0+$', ''), '')::INTEGER AS responsible_mop_id,
    
    -- Даты создания/изменения
    CASE WHEN d.date_create ~ '^\d{4}-\d{2}-\d{2}' THEN d.date_create::TIMESTAMP END AS date_create,
    CASE WHEN d.date_modify ~ '^\d{4}-\d{2}-\d{2}' THEN d.date_modify::TIMESTAMP END AS date_modify,
    
    -- Даты этапов
    CASE WHEN u.uf_fact_presentation_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_fact_presentation_date::DATE END AS show_1_dt,
    CASE WHEN u.uf_crm_booking_contract_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_crm_booking_contract_date::DATE END AS reserve_dt,
    CASE WHEN u.uf_crm_contract_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_crm_contract_date::DATE END AS deal_1_dt,
    CASE WHEN u.uf_plan_showing_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_plan_showing_date::DATE END AS appointed_shows_dt,
    CASE WHEN u.UF_CRM_DOWN_PAYMENT_DATE ~ '^\d{4}-\d{2}-\d{2}' THEN u.UF_CRM_DOWN_PAYMENT_DATE::DATE END AS pv_paid_dt,
    
    -- Финансы
    CASE WHEN u.uf_crm_1690293096296 ~ '^[0-9]+\.?[0-9]*$' THEN u.uf_crm_1690293096296::NUMERIC END AS deal_amount,
    CASE WHEN u.uf_booked_price ~ '^[0-9]+\.?[0-9]*$' THEN u.uf_booked_price::NUMERIC END AS reserve_lot_sum,
    CASE WHEN d.opportunity ~ '^[0-9]+\.?[0-9]*$' THEN d.opportunity::NUMERIC END AS opportunity,
    
    -- Объект и прочее
    TRIM(u.uf_crm_object) AS deal_object,
    CASE WHEN u.uf_area ~ '^[0-9]+\.?[0-9]*$' THEN u.uf_area::NUMERIC END AS area,
    TRIM(u.uf_crm_agency) AS agency,
    TRIM(u.uf_crm_agency) AS uf_crm_agency,
    TRIM(u.uf_crm_agent) AS uf_crm_agent,
    NULLIF(REGEXP_REPLACE(u.uf_crm_sales_deal, '\.0+$', ''), '')::INTEGER AS sales_deal_id,
    TRIM(d.source_id) AS source_id,
    
    -- Вычисляемые даты
CASE WHEN u.uf_fact_presentation_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_fact_presentation_date::DATE END AS show_dt,
CASE WHEN u.uf_crm_contract_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_crm_contract_date::DATE END AS deal_dt,
    CASE WHEN u.uf_plan_showing_date ~ '^\d{4}-\d{2}-\d{2}' THEN u.uf_plan_showing_date::DATE END AS appointed_show_dt,
    
    -- Причина ЗИН (расшифровка через справочник)
    COALESCE(zin_aop.value, zin_direct.value) AS zin_reason,
--4 цифры
NULLIF(TRIM(u.uf_last_4_phone_numbers), '') AS last_4_phone,
    
    -- Флаги
    CASE WHEN LOWER(d.title) LIKE '%тест%' OR LOWER(d.title) LIKE '%test%' THEN TRUE ELSE FALSE END AS is_test,
    CASE WHEN u.uf_fact_presentation_date ~ '^\d{4}-\d{2}-\d{2}' THEN TRUE ELSE FALSE END AS has_show,
    CASE WHEN u.uf_crm_booking_contract_date ~ '^\d{4}-\d{2}-\d{2}' THEN TRUE ELSE FALSE END AS has_reserve,
    CASE WHEN u.uf_crm_contract_date ~ '^\d{4}-\d{2}-\d{2}' THEN TRUE ELSE FALSE END AS has_deal,
    CASE WHEN u.uf_plan_showing_date ~ '^\d{4}-\d{2}-\d{2}' THEN TRUE ELSE FALSE END AS has_appointed_show,
    
    -- Сырые данные для отладки
    u.uf_fact_presentation_date AS _raw_show_1,
    u.uf_crm_booking_contract_date AS _raw_reserve,
    u.uf_crm_contract_date AS _raw_deal_1,
    u.uf_crm_1690293096296 AS _raw_deal_amount,
    u.uf_booked_price AS _raw_reserve_lot_sum,
    u.uf_area AS _raw_area,
    u.uf_crm_responsible_mop AS _raw_responsible_mop,
    u.uf_plan_showing_date AS _raw_appointed_show_1,
    NULL AS _raw_appointed_show_2,
    COALESCE(u.uf_refusal_reason, u.uf_crm_1692888387831) AS _raw_zin_reason,
    
    -- ETL метаданные
    NOW() AS _etl_loaded_at,
    'raw.b_crm_deal + raw.b_uts_crm_deal' AS _etl_source_table

FROM raw.b_crm_deal d
LEFT JOIN raw.b_uts_crm_deal u ON d.id = u.value_id
-- JOIN для расшифровки причины ЗИН (АОП, category_id=5, user_field_id=166)
LEFT JOIN raw.b_user_field_enum zin_aop 
    ON NULLIF(REGEXP_REPLACE(u.uf_refusal_reason, '\.0+$', ''), '') = zin_aop.id::TEXT 
    AND zin_aop.user_field_id = '166'
-- JOIN для расшифровки причины ЗИН (Прямые, category_id=10, user_field_id=3298)
LEFT JOIN raw.b_user_field_enum zin_direct 
    ON NULLIF(REGEXP_REPLACE(u.uf_crm_1692888387831, '\.0+$', ''), '') = zin_direct.id::TEXT 
    AND zin_direct.user_field_id = '3298'
WHERE d.category_id IN ('4', '5', '10');

-- Проверка
SELECT 
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE is_test) AS test_deals,
    COUNT(*) FILTER (WHERE has_show) AS with_show,
    COUNT(*) FILTER (WHERE has_reserve) AS with_reserve,
    COUNT(*) FILTER (WHERE has_deal) AS with_deal,
    COUNT(*) FILTER (WHERE has_appointed_show) AS with_appointed_show,
    COUNT(*) FILTER (WHERE zin_reason IS NOT NULL) AS with_zin_reason
FROM staging.b_crm_deal;