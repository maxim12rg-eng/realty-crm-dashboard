-- ============================================================
-- BOT: bot.responsibles
-- Источник: staging.mops
-- Режим: UPSERT (без удаления, сохраняем историю)
-- ============================================================
CREATE TABLE IF NOT EXISTS bot.responsibles (
    mop_id BIGINT PRIMARY KEY,
    mop_name TEXT
);
INSERT INTO bot.responsibles (mop_id, mop_name)
SELECT mop_id, mop_name
FROM staging.mops
ON CONFLICT (mop_id) DO UPDATE SET
    mop_name = EXCLUDED.mop_name;

-- Проверка
SELECT COUNT(*) as total_rows FROM bot.responsibles;