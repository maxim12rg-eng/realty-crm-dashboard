-- ============================================================
-- STAGING: staging.staging_deal_comments
-- Источник: raw.b_crm_timeline + raw.b_crm_timeline_bind
-- Комментарии к сделкам
-- ============================================================

DROP TABLE IF EXISTS staging.staging_deal_comments;

CREATE TABLE staging.staging_deal_comments AS
SELECT
    bctb.entity_id::int   AS deal_id,
    bct.comment            AS comment
FROM raw.b_crm_timeline bct
JOIN raw.b_crm_timeline_bind bctb
    ON bct.id = bctb.owner_id
WHERE bctb.entity_type_id::int = 2
  AND bct.associated_entity_type_id::int = 0
  AND bct.settings <> 'a:0:{}'   AND bct.comment IS NOT NULL
  AND TRIM(bct.comment) <> ''
ORDER BY bctb.entity_id::int DESC;

SELECT COUNT(*) AS cnt FROM staging.staging_deal_comments;

