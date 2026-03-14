-- ============================================================
-- STAGING: staging.b_crm_status
-- Источник: raw.b_crm_status
-- Связь: status_id = b_crm_deal.stage_id
--        entity_id (DEAL_STAGE_5, DEAL_STAGE_10) = category_id сделки
-- ============================================================


CREATE TABLE IF NOT EXISTS staging.b_crm_status (
    id INTEGER,
    entity_id TEXT,
    status_id TEXT,
    name TEXT,
    name_init TEXT,
    sort INTEGER,
    system BOOLEAN,
    color TEXT,
    semantics TEXT,
    category_id INTEGER,
    _etl_loaded_at TIMESTAMP,
    _etl_source_table TEXT,
    PRIMARY KEY (entity_id, status_id)
);

TRUNCATE TABLE staging.b_crm_status;

INSERT INTO staging.b_crm_status (
    id, entity_id, status_id, name, name_init,
    sort, system, color, semantics, category_id,
    _etl_loaded_at, _etl_source_table
)
SELECT 
    NULLIF(TRIM(s.id), '')::INTEGER AS id,
    TRIM(s.entity_id) AS entity_id,
    TRIM(s.status_id) AS status_id,
    TRIM(s.name) AS name,
    TRIM(s.name_init) AS name_init,
    NULLIF(TRIM(s.sort), '')::INTEGER AS sort,
    CASE WHEN UPPER(TRIM(s.system)) = 'Y' THEN TRUE ELSE FALSE END AS system,
    TRIM(s.color) AS color,
    NULLIF(TRIM(s.semantics), '') AS semantics,
    NULLIF(TRIM(s.category_id), '')::INTEGER AS category_id,
    NOW() AS _etl_loaded_at,
    'raw.b_crm_status' AS _etl_source_table
FROM raw.b_crm_status s;

-- Проверка
SELECT entity_id, COUNT(*) as cnt
FROM staging.b_crm_status
GROUP BY entity_id
ORDER BY entity_id;
