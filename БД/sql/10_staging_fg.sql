-- 10_staging_fg.sql
-- Staging слой для таблицы контроля качества показов (FG)
-- Источник: raw.raw_fg (из Tager Excel)

DROP TABLE IF EXISTS staging.staging_fg;

CREATE TABLE staging.staging_fg AS
SELECT
    -- Основные поля
    CASE 
        WHEN дата ~ '^\d{4}-\d{2}-\d{2}' THEN дата::DATE 
        ELSE NULL 
    END AS дата,
    длина,
    менеджер,
    теплота,
    
    -- Оценки
    NULLIF(энергичный_настрой, '')::NUMERIC AS энергичный_настрой,
    NULLIF(приветствие, '')::NUMERIC AS приветствие,
    NULLIF(имя, '')::NUMERIC AS имя,
    NULLIF(забота, '')::NUMERIC AS забота,
    NULLIF(установление_контакта, '')::NUMERIC AS установление_контакта,
    NULLIF(фрейминг, '')::NUMERIC AS фрейминг,
    NULLIF(что_смотрели, '')::NUMERIC AS что_смотрели,
    NULLIF(программирование, '')::NUMERIC AS программирование,
    NULLIF(цель_покупки, '')::NUMERIC AS цель_покупки,
    NULLIF(квалификация, '')::NUMERIC AS квалификация,
    NULLIF(потребность, '')::NUMERIC AS потребность,
    NULLIF(боль, '')::NUMERIC AS боль,
    NULLIF(выход_на_лпр_работа_с_лпр, '')::NUMERIC AS выход_на_лпр_работа_с_лпр,
    NULLIF(резюмирование, '')::NUMERIC AS резюмирование,
    NULLIF(презентация, '')::NUMERIC AS презентация,
    NULLIF(предзакрытие, '')::NUMERIC AS предзакрытие,
    NULLIF(индивидуальный_оффер, '')::NUMERIC AS индивидуальный_оффер,
    NULLIF(кэв, '')::NUMERIC AS кэв,
    NULLIF(ускоритель_1_дефицит, '')::NUMERIC AS ускоритель_1_дефицит,
    NULLIF(как_проходит_сделка_бронь, '')::NUMERIC AS как_проходит_сделка_бронь,
    NULLIF(следующий_шаг, '')::NUMERIC AS следующий_шаг,
    NULLIF(соблюдение_структуры_встречи, '')::NUMERIC AS соблюдение_структуры_встречи,
    NULLIF(инвестиционная_привлекательность, '')::NUMERIC AS инвестиционная_привлекательность,
    NULLIF(ответы_на_вопросы, '')::NUMERIC AS ответы_на_вопросы,
    NULLIF(речь, '')::NUMERIC AS речь,
    NULLIF(качество_записи, '')::NUMERIC AS качество_записи,
    
    -- Квалификационные поля
    NULLIF(как_часто_планируете_приезжать, '')::NUMERIC AS как_часто_планируете_приезжать,
    NULLIF(срок_приобретения_к, '')::NUMERIC AS срок_приобретения_к,
    NULLIF(бюджет_к, '')::NUMERIC AS бюджет_к,
    NULLIF(способ_приобретения_наличные_ипот, '')::NUMERIC AS способ_приобретения_наличные_ипот,
    NULLIF(лпр_к, '')::NUMERIC AS лпр_к,
    NULLIF(есть_ли_опыт_инвестирования_желае, '')::NUMERIC AS есть_ли_опыт_инвестирования_желае,
    NULLIF(цель_инвестирования_краткосрочны, '')::NUMERIC AS цель_инвестирования_краткосрочны,
    
    -- Итоговые баллы
    NULLIF(неучтенное, '')::NUMERIC AS неучтенное,
    NULLIF(подсчет_баллов, '')::NUMERIC AS подсчет_баллов,
    ROUND(NULLIF(fg, '')::NUMERIC, 2) AS fg,
    
    -- Бронь
    бронь,
    дата_брони,
    
    -- Дополнительные поля
    "unnamed:_42" AS unnamed_42,
    оценка_возражений,
    возражения,
    "unnamed:_45" AS unnamed_45,
    
    -- Временные поля
    неделя,
    день,
    месяц,
    год,
    
    -- Прочее
    комментарий,
    CASE 
        WHEN сделка IS NOT NULL AND сделка != '' AND сделка != 'None'
        THEN (REGEXP_MATCH(сделка, '/(\d+)/'))[1]::INTEGER 
        ELSE NULL 
    END AS id_сделки,
    оценка,
    отдел_продаж,
    асессор,
    
    -- Метаданные
    source_file,
    loaded_at

FROM raw.raw_fg
WHERE дата IS NOT NULL
  AND дата != 'Дата'
  AND дата != 'None';
