-- ============================================================
-- STAGING: staging.cmap_deals
-- Источник: raw.raw_cmap_deals (bnMAP.pro API)
-- ============================================================

DROP TABLE IF EXISTS staging.cmap_deals;

CREATE TABLE staging.cmap_deals AS
SELECT
    TRIM(hc_id_hash)                    AS id_object,
    TRIM(hc_name)                       AS object,
    TRIM(city)                          AS city,
    TRIM(loc_district)                  AS loc_district,
    TRIM(class)                         AS class,
    TRIM(developer)                     AS developer,
    CASE WHEN agreement_date ~ '^\d{4}-\d{2}-\d{2}' 
         THEN agreement_date::DATE END  AS date_sold,
    TRIM(ot_name)                       AS type_lot,
    NULLIF(TRIM(floor), '')::INTEGER    AS floor,
    NULLIF(TRIM(do_square), '')::NUMERIC(12,2)    AS do_square,
    NULLIF(TRIM(est_budget), '')::NUMERIC(15,2)   AS est_budget,
    NULLIF(TRIM(price_square_r), '')::NUMERIC(12,2) AS price_square_r,
    TRIM(interior)                      AS interior,
    TRIM(bt_name)                       AS bt_name,
    TRIM(mortgage)                      AS ipoteka,
    TRIM(bank_name)                     AS bank_name,
    NULLIF(TRIM(mortgage_term), '')::INTEGER AS ipoteka_month,
    NOW()                               AS _etl_loaded_at
FROM raw.raw_cmap_deals
WHERE TRIM(ot_name) IN ('квартира', 'нежилое помещение', 'апартаменты');
