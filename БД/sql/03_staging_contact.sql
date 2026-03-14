-- ============================================================
-- STAGING: staging.b_crm_contact
-- Источник: raw.b_crm_contact
-- ============================================================

DROP TABLE IF EXISTS staging.b_crm_contact;

CREATE TABLE staging.b_crm_contact AS
SELECT 
    NULLIF(TRIM(c.id), '')::INTEGER AS contact_id,
    TRIM(c.full_name) AS full_name,
    TRIM(c.name) AS first_name,
    TRIM(c.last_name) AS last_name,
    TRIM(c.second_name) AS middle_name,
    NULLIF(REGEXP_REPLACE(c.company_id, '\.0+$', ''), '')::INTEGER AS company_id,
    TRIM(c.type_id) AS type_id,
    NULLIF(TRIM(c.created_by_id), '')::INTEGER AS created_by_id,
    NULLIF(TRIM(c.modify_by_id), '')::INTEGER AS modify_by_id,
    NULLIF(TRIM(c.assigned_by_id), '')::INTEGER AS assigned_by_id,
    CASE WHEN c.date_create ~ '^\d{4}-\d{2}-\d{2}' THEN c.date_create::TIMESTAMP END AS date_create,
    CASE WHEN c.date_modify ~ '^\d{4}-\d{2}-\d{2}' THEN c.date_modify::TIMESTAMP END AS date_modify,
    CASE WHEN c.last_activity_time ~ '^\d{4}-\d{2}-\d{2}' THEN c.last_activity_time::TIMESTAMP END AS last_activity_time,
    TRIM(c.source_id) AS source_id,
    TRIM(c.source_description) AS source_description,
    TRIM(c.post) AS post,
    TRIM(c.address) AS address,
    TRIM(c.comments) AS comments,
    CASE WHEN UPPER(TRIM(c.has_phone)) = 'Y' THEN TRUE ELSE FALSE END AS has_phone,
    CASE WHEN UPPER(TRIM(c.has_email)) = 'Y' THEN TRUE ELSE FALSE END AS has_email,
    NULLIF(TRIM(c.category_id), '')::INTEGER AS category_id,
    NOW() AS _etl_loaded_at,
    'raw.b_crm_contact' AS _etl_source_table
FROM raw.b_crm_contact c;