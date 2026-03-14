-- ============================================================
-- STAGING: staging.mops
-- Источник: raw.b_iblock_element (iblock_id = 85)
-- ============================================================

DROP TABLE IF EXISTS staging.mops;

CREATE TABLE staging.mops AS
SELECT 
    NULLIF(TRIM(e.id), '')::BIGINT AS mop_id,
    TRIM(e.name) AS mop_name,
    TRIM(e.code) AS code,
    TRIM(e.xml_id) AS xml_id,
    CASE WHEN UPPER(TRIM(e.active)) = 'Y' THEN TRUE ELSE FALSE END AS is_active,
    NULLIF(TRIM(e.sort), '')::INTEGER AS sort_order,
    CASE WHEN e.date_create ~ '^\d{4}-\d{2}-\d{2}' THEN e.date_create::TIMESTAMP END AS date_create,
    CASE WHEN e.timestamp_x ~ '^\d{4}-\d{2}-\d{2}' THEN e.timestamp_x::TIMESTAMP END AS date_modify,
    CASE WHEN e.active_from ~ '^\d{4}-\d{2}-\d{2}' THEN e.active_from::TIMESTAMP END AS active_from,
    CASE WHEN e.active_to ~ '^\d{4}-\d{2}-\d{2}' THEN e.active_to::TIMESTAMP END AS active_to,
    NULLIF(REGEXP_REPLACE(e.created_by, '\.0+$', ''), '')::INTEGER AS created_by,
    NULLIF(REGEXP_REPLACE(e.modified_by, '\.0+$', ''), '')::INTEGER AS modified_by,
    e.id AS _raw_id,
    e.name AS _raw_name,
    e.active AS _raw_active,
    NOW() AS _etl_loaded_at,
    'raw.b_iblock_element' AS _etl_source_table
FROM raw.b_iblock_element e
WHERE e.iblock_id = '85';