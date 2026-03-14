-- ============================================================
-- STAGING: staging.b_crm_company
-- Источник: raw.b_crm_company
-- ============================================================

DROP TABLE IF EXISTS staging.b_crm_company;

CREATE TABLE staging.b_crm_company AS
SELECT 
    NULLIF(TRIM(c.id), '')::BIGINT AS company_id,
    TRIM(c.title) AS title,
    TRIM(c.company_type) AS company_type,
    TRIM(c.industry) AS industry,
    NULLIF(TRIM(c.category_id), '')::INTEGER AS category_id,
    NULLIF(TRIM(c.created_by_id), '')::INTEGER AS created_by_id,
    NULLIF(TRIM(c.modify_by_id), '')::INTEGER AS modify_by_id,
    NULLIF(TRIM(c.assigned_by_id), '')::INTEGER AS assigned_by_id,
    CASE WHEN c.date_create ~ '^\d{4}-\d{2}-\d{2}' THEN c.date_create::TIMESTAMP END AS date_create,
    CASE WHEN c.date_modify ~ '^\d{4}-\d{2}-\d{2}' THEN c.date_modify::TIMESTAMP END AS date_modify,
    CASE WHEN c.last_activity_time ~ '^\d{4}-\d{2}-\d{2}' THEN c.last_activity_time::TIMESTAMP END AS last_activity_time,
    TRIM(c.address) AS address,
    TRIM(c.address_legal) AS address_legal,
    TRIM(c.banking_details) AS banking_details,
    CASE WHEN UPPER(TRIM(c.has_phone)) = 'Y' THEN TRUE ELSE FALSE END AS has_phone,
    CASE WHEN UPPER(TRIM(c.has_email)) = 'Y' THEN TRUE ELSE FALSE END AS has_email,
    CASE WHEN UPPER(TRIM(c.is_my_company)) = 'Y' THEN TRUE ELSE FALSE END AS is_my_company,
    NULLIF(REGEXP_REPLACE(c.revenue, '\.0+$', ''), '')::NUMERIC AS revenue,
    TRIM(c.currency_id) AS currency_id,
    TRIM(c.employees) AS employees,
    TRIM(c.comments) AS comments,
    NULLIF(REGEXP_REPLACE(c.lead_id, '\.0+$', ''), '')::INTEGER AS lead_id,
    NULLIF(REGEXP_REPLACE(c.last_activity_by, '\.0+$', ''), '')::INTEGER AS last_activity_by,
    NOW() AS _etl_loaded_at,
    'raw.b_crm_company' AS _etl_source_table
FROM raw.b_crm_company c;