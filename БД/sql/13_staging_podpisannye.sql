-- ============================================================
-- STAGING: staging.staging_podpisannye
-- Источник: raw.raw_podpisannye (лист "ПОДПИСАННЫЕ")
-- ============================================================

DROP TABLE IF EXISTS staging.staging_podpisannye;

CREATE TABLE staging.staging_podpisannye AS
SELECT
    NULLIF(REPLACE(REPLACE(TRIM(num), '-', ''), ',', '.'), '')::INTEGER AS num,
    CASE WHEN TRIM(дата_брони) ~ '^\d{4}-\d{2}-\d{2}'
         THEN TRIM(дата_брони)::DATE
         ELSE NULL
    END                                                     AS дата_брони,
    NULLIF(TRIM(объект), '')                                AS объект,
    NULLIF(TRIM(номер_лота), '')                            AS номер_лота,
    NULLIF(TRIM(здание), '')                                AS здание,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(s_факт), '-', ''), ',', '.'), '')::NUMERIC, 2) AS s_факт,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(s_по_выписке_проектe), '-', ''), ',', '.'), '')::NUMERIC, 2) AS s_по_выписке_проектe,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(s_террасы), '-', ''), ',', '.'), '')::NUMERIC, 2) AS s_террасы,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(сумма_сделки), '-', ''), ',', '.'), '')::NUMERIC, 2) AS сумма_сделки,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(цена_за_кв_м), '-', ''), ',', '.'), '')::NUMERIC, 2) AS цена_за_кв_м,
    NULLIF(TRIM(клиент), '')                                AS клиент,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(размер_пв), '-', ''), ',', '.'), '')::NUMERIC, 4) AS размер_пв,
    ROUND(NULLIF(REPLACE(REPLACE(TRIM(срок_рассрочки_мес), '-', ''), ',', '.'), '')::NUMERIC, 0) AS срок_рассрочки_мес,
    NULLIF(REPLACE(TRIM(порядок_рассрочки), '-', ''), '')   AS порядок_рассрочки,
    NULLIF(REPLACE(TRIM(спец_условия), '-', ''), '')        AS спец_условия,
    NULLIF(TRIM(мос_брокер), '')                            AS мос_брокер,
    NULLIF(TRIM(менеджер), '')                              AS менеджер,
    NULLIF(TRIM(агент), '')                                 AS агент,
    NULLIF(TRIM(агентство), '')                             AS агентство,
NULLIF(REPLACE(TRIM(размер_комисси), '-', ''), '') AS размер_комиссии,
NULLIF(REPLACE(TRIM(оплачено_комиссии), '-', ''), '') AS оплачено_комиссии,
    CASE WHEN TRIM(дата_в_договоре) ~ '^\d{4}-\d{2}-\d{2}'
         THEN TRIM(дата_в_договоре)::DATE
         ELSE NULL
    END                                                     AS дата_в_договоре,
    CASE WHEN TRIM(дата_регистрации) ~ '^\d{4}-\d{2}-\d{2}'
         THEN TRIM(дата_регистрации)::DATE
         ELSE NULL
    END                                                     AS дата_регистрации,
    CASE WHEN TRIM(дата_пв) ~ '^\d{4}-\d{2}-\d{2}'
         THEN TRIM(дата_пв)::DATE
         ELSE NULL
    END                                                     AS дата_пв,
    NULLIF(REPLACE(REPLACE(TRIM(количество_рабочих_дней_в_работе), '-', ''), ',', '.'), '')::INTEGER AS количество_рабочих_дней_в_работе,
    NULLIF(TRIM(ссылка), '')                                AS ссылка,
    NULLIF(TRIM(комментарий), '')                           AS комментарий,
    NOW()                                                   AS _etl_loaded_at,
    'raw.raw_podpisannye'                                   AS _etl_source_table
FROM raw.raw_podpisannye;

-- Проверка
SELECT COUNT(*) AS cnt FROM staging.staging_podpisannye;
