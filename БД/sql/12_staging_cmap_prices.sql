-- ============================================================
-- STAGING: staging.cmap_prices
-- Источник: raw.raw_cmap_prices (bnMAP.pro API — Прайсы)
-- ============================================================

DROP TABLE IF EXISTS staging.cmap_prices;

CREATE TABLE staging.cmap_prices AS
SELECT
    TRIM(id)                                AS id,
    TRIM(hc_name)                           AS project_name,
    TRIM(hc_id_hash)                        AS id_project,
    TRIM(developer)                         AS developer,
    TRIM(builder)                           AS builder,
    TRIM(region)                            AS region,
    TRIM(city)                              AS city,
    TRIM(district)                          AS district,
    TRIM(building)                          AS building,
    TRIM(b_id_hash)                         AS id_building,
    NULLIF(TRIM(section), '0')::INTEGER     AS section,
    NULLIF(TRIM(floor), '')::INTEGER        AS floor,
    TRIM(numapartment)                      AS num_apartment,
    TRIM(rooms)                             AS rooms,
    NULLIF(TRIM(square), '')::NUMERIC(12,3) AS square,
    NULLIF(TRIM(squareprice), '')::NUMERIC(12,2)    AS price_sqm,
    NULLIF(TRIM(price), '')::NUMERIC(15,2)          AS price,
    TRIM(interior)                          AS interior,
    TRIM(object_type)                       AS object_type,
    TRIM(object_class)                      AS object_class,
    TRIM(discount)                          AS has_discount,
    NULLIF(TRIM(discount_value), '')::NUMERIC(15,2) AS discount_value,
    TRIM(agreement)                         AS agreement_type,
    CASE WHEN dsc ~ '^\d{4}-\d{2}-\d{2}' AND dsc <> '0000-00-00'
         THEN dsc::DATE END                 AS date_commission,
    CASE WHEN start_sales ~ '^\d{4}-\d{2}-\d{2}' AND start_sales <> '0000-00-00'
         THEN start_sales::DATE END         AS date_start_sales,
    TRIM(stage)                             AS construction_stage,
    TRIM(construction)                      AS construction_type,
    CASE WHEN first_lot_date ~ '^\d{4}-\d{2}-\d{2}' AND first_lot_date <> '0000-00-00'
         THEN first_lot_date::DATE END      AS first_lot_date,
    NULLIF(TRIM(first_lot_price_square), '')::NUMERIC(12,2) AS first_lot_price_sqm,
    NULLIF(TRIM(first_lot_price), '')::NUMERIC(15,2)        AS first_lot_price,
    NULLIF(TRIM(lot_term), '')::INTEGER     AS lot_term_days,
    NULLIF(TRIM(lot_price_square_rise), '')::NUMERIC(12,6)  AS price_sqm_rise,
    NULLIF(TRIM(lot_price_rise), '')::NUMERIC(12,6)         AS price_rise,
    TRIM(first_appearance)                  AS first_appearance,
    CASE WHEN create_time ~ '^\d{4}-\d{2}-\d{2}'
         THEN create_time::TIMESTAMP END    AS date_collected,
    TRIM(a_lot_hash)                        AS id_lot,
    TRIM(pbo_id_hash)                       AS id_pbo,
    NOW()                                   AS _etl_loaded_at
FROM raw.raw_cmap_prices;