# etl_full_v3.py
# Полный ETL: RAW (инкрементально) → staging → mart → bot
# Исправлена проблема с памятью: чанковая загрузка больших таблиц
# Атомарная загрузка: через временные таблицы (либо всё, либо ничего)
# Запуск каждые 10 минут

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
import schedule
import time
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import threading
import gc

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

SQL_DIR = BASE_DIR / 'sql'

# Telegram алерты
BOT_TOKEN = os.getenv("ETL_BOT_TOKEN")
ALERT_CHAT_IDS = [
    890703314,  # Лобанов Максим
]

# Таймауты
SQL_SCRIPT_TIMEOUT = 60  # секунд на один SQL-скрипт

# Размер чанка для больших таблиц
CHUNK_SIZE = 10000

# Порядок выполнения SQL-скриптов
SQL_SCRIPTS = [
    '01_staging_mops.sql',
    '02_staging_company.sql',
    '03_staging_contact.sql',
    '04_staging_deal.sql',
    '05_mart_deals_funnel.sql',
    '08_mart_deals.sql',
    '06_bot_deal_facts.sql',
    '07_bot_responsibles.sql',
    '09_staging_status.sql',
    '10_staging_fg.sql',
    '11_staging_cmap_deals.sql',
    '12_staging_cmap_prices.sql',
    '13_staging_podpisannye.sql',
    '14_staging_v_rabote.sql',
    '15_staging_deal_comments.sql',
    '16_rang_an.sql',   
    '17_rang_agent.sql',
]
# Зависимости SQL-скриптов от RAW-таблиц
# Если таблицы-источника нет в БД — скрипт пропускается без ошибки
SQL_DEPENDENCIES = {
    '10_staging_fg.sql': ['raw.raw_fg'],
    '13_staging_podpisannye.sql': ['raw.raw_podpisannye'],
    '14_staging_v_rabote.sql': ['raw.raw_v_rabote'],
    '15_staging_deal_comments.sql': ['raw.b_crm_timeline'],
}

# Конфигурация таблиц RAW
TABLES_CONFIG = {
    # ИНКРЕМЕНТАЛЬНЫЕ по дате
    'b_crm_deal': {'mode': 'incremental', 'field': 'date_modify', 'order': 1},
    'b_crm_lead': {'mode': 'incremental', 'field': 'date_modify', 'order': 2},
    'b_crm_contact': {'mode': 'incremental', 'field': 'date_modify', 'order': 3},
    'b_crm_company': {'mode': 'incremental', 'field': 'date_modify', 'order': 4},
    'b_iblock_element': {'mode': 'incremental', 'field': 'timestamp_x', 'order': 5},
    'b_crm_timeline': {'mode': 'incremental_chunked', 'field': 'created', 'order': 6},
    'b_crm_act': {'mode': 'incremental', 'field': 'last_updated', 'order': 7},
    'b_voximplant_statistic': {'mode': 'incremental', 'field': 'call_start_date', 'order': 8},
    
    # UTS-таблицы — FULL REFRESH с чанками и атомарностью
    'b_uts_crm_deal': {'mode': 'full_chunked', 'order': 9},
    'b_uts_crm_lead': {'mode': 'full_chunked', 'order': 10},
    'b_uts_crm_contact': {'mode': 'full_chunked', 'order': 11},
    
    # ИНКРЕМЕНТАЛЬНЫЕ по ID
    'b_iblock_element_property': {'mode': 'incremental_id', 'field': 'id', 'order': 12},
    
    # FULL REFRESH (справочники и небольшие таблицы)
    'b_crm_timeline_bind': {'mode': 'full_chunked', 'order': 13},
    'b_crm_deal_stage_history': {'mode': 'full_chunked', 'order': 14},
    'b_crm_lead_status_history': {'mode': 'full', 'order': 15},
    'b_crm_field_multi': {'mode': 'full', 'order': 16},
    'b_crm_utm': {'mode': 'full', 'order': 17},
    'b_user': {'mode': 'full', 'order': 18},
    'b_user_field': {'mode': 'full', 'order': 19},
    'b_user_field_enum': {'mode': 'full', 'order': 20},
    'b_iblock': {'mode': 'full', 'order': 21},
    'b_iblock_fields': {'mode': 'full', 'order': 22},
    'b_crm_status': {'mode': 'full', 'order': 23},
    'b_crm_deal_category': {'mode': 'full', 'order': 24},
    'b_crm_dynamic_items_184': {'mode': 'full', 'order': 25},
    'b_crm_dynamic_items_164': {'mode': 'full', 'order': 26},
    'b_crm_dynamic_items_186': {'mode': 'full', 'order': 27},
    'b_crm_dynamic_items_1054': {'mode': 'full', 'order': 28},
}

# ============================================================
# TELEGRAM АЛЕРТЫ
# ============================================================

def send_telegram_alert(message):
    """Отправляет алерт в Telegram"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    for chat_id in ALERT_CHAT_IDS:
        try:
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"   ⚠️ Не удалось отправить алерт в Telegram: {e}")

# ============================================================
# ПОДКЛЮЧЕНИЯ
# ============================================================

def get_mysql_engine():
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    port = os.getenv('MYSQL_PORT')
    database = os.getenv('MYSQL_DATABASE')
    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return create_engine(
        url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            'connect_timeout': 30,
            'read_timeout': 600,
            'write_timeout': 600,
        }
    )

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv('PG_HOST'),
        port=int(os.getenv('PG_PORT')),
        database=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD')
    )

def get_pg_engine():
    user = os.getenv('PG_USER')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    database = os.getenv('PG_DATABASE')
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)

def ensure_schemas():
    """Создаёт схемы в PostgreSQL, если они ещё не существуют"""
    schemas = ['raw', 'staging', 'mart', 'bot']
    conn = get_pg_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for schema in schemas:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"✅ Схемы проверены/созданы: {', '.join(schemas)}")
    finally:
        conn.close()

def ensure_bot_tables():
    """Создаёт bot-таблицы и заполняет начальными данными"""
    conn = get_pg_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Создание таблиц
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot.manager_weights (
                    id SERIAL PRIMARY KEY,
                    mop_id INTEGER NOT NULL,
                    mop_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    weight_deals NUMERIC NOT NULL,
                    weight_reserves NUMERIC NOT NULL,
                    weight_shows NUMERIC NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS bot.company_plans (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    group_name TEXT NOT NULL,
                    target_revenue BIGINT,
                    target_deals INTEGER,
                    target_reserve_revenue BIGINT,
                    target_reserves INTEGER,
                    target_shows INTEGER,
                    target_pv_paid BIGINT,
                    coef_contract_from_pv NUMERIC,
                    coef_avg_deal_size BIGINT,
                    coef_reserve_from_contract NUMERIC,
                    coef_avg_reserve_size BIGINT,
                    coef_show_conversion NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS bot.scheduler_control (
                    id INTEGER NOT NULL DEFAULT 1,
                    is_active BOOLEAN DEFAULT TRUE,
                    paused_by BIGINT,
                    paused_at TIMESTAMP,
                    resumed_by BIGINT,
                    resumed_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS bot.manager_settings (
                    id SERIAL PRIMARY KEY,
                    mop_id INTEGER NOT NULL,
                    mop_name TEXT NOT NULL,
                    telegram_chat_id BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    receive_personal_reports BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # scheduler_control
            cur.execute("""
                INSERT INTO bot.scheduler_control (id, is_active)
                SELECT 1, FALSE
                WHERE NOT EXISTS (SELECT 1 FROM bot.scheduler_control WHERE id = 1);
            """)

            # manager_weights — наши синтетические МОПы
            cur.execute("""
                INSERT INTO bot.manager_weights (id, mop_id, mop_name, group_name, weight_deals, weight_reserves, weight_shows, is_active)
                SELECT * FROM (VALUES
                    (1, 2544100, 'Иванов Иван',      'ОП Волна', 0.273, 0.273, 0.273, TRUE),
                    (2, 2544101, 'Петрова Мария',     'ОП Волна', 0.273, 0.273, 0.273, TRUE),
                    (3, 2544102, 'Сидоров Алексей',   'ОП Волна', 0.273, 0.273, 0.273, TRUE),
                    (4, 2544103, 'Козлова Наталья',   'ОП Волна', 0.182, 0.182, 0.182, TRUE),
                    (5, 2544104, 'Новиков Дмитрий',   'Прямые продажи', 1.000, 1.000, 1.000, TRUE),
                    (6, 2544105, 'Морозова Елена',    'Переуступки', 1.000, 1.000, 1.000, TRUE),
                    (7, 2544106, 'Волков Сергей',     'ОП Волна', 0.273, 0.273, 0.273, TRUE),
                    (8, 2544107, 'Соколова Ирина',    'ОП Волна', 0.273, 0.273, 0.273, TRUE),
                    (9, 2544108, 'Лебедев Андрей',    'ОП Волна', 0.182, 0.182, 0.182, TRUE),
                    (10, 2544109, 'Попова Анна',      'ОП Волна', 0.182, 0.182, 0.182, TRUE)
                ) AS v(id, mop_id, mop_name, group_name, weight_deals, weight_reserves, weight_shows, is_active)
                WHERE NOT EXISTS (SELECT 1 FROM bot.manager_weights LIMIT 1);
            """)
            cur.execute("SELECT setval('bot.manager_weights_id_seq', 10);")

            # company_plans — синтетические планы
            cur.execute("""
                INSERT INTO bot.company_plans (id, year, month, group_name, target_revenue, target_deals, target_reserve_revenue, target_reserves, target_shows, target_pv_paid, coef_contract_from_pv, coef_avg_deal_size, coef_reserve_from_contract, coef_avg_reserve_size, coef_show_conversion)
                SELECT * FROM (VALUES
                    (1, 2025, 4,  'ОП Волна', 500000000::BIGINT, 15, 600000000::BIGINT, 18, 100, 300000000::BIGINT, 1.500, 30000000::BIGINT, 1.200, 30000000::BIGINT, 0.150),
                    (2, 2025, 5,  'ОП Волна', 550000000::BIGINT, 16, 660000000::BIGINT, 20, 110, 320000000::BIGINT, 1.500, 32000000::BIGINT, 1.200, 32000000::BIGINT, 0.160),
                    (3, 2025, 6,  'ОП Волна', 600000000::BIGINT, 18, 720000000::BIGINT, 22, 120, 350000000::BIGINT, 1.600, 33000000::BIGINT, 1.200, 33000000::BIGINT, 0.160),
                    (4, 2025, 7,  'ОП Волна', 580000000::BIGINT, 17, 696000000::BIGINT, 21, 115, 340000000::BIGINT, 1.600, 33000000::BIGINT, 1.200, 33000000::BIGINT, 0.155),
                    (5, 2025, 8,  'ОП Волна', 560000000::BIGINT, 16, 672000000::BIGINT, 20, 112, 330000000::BIGINT, 1.550, 32000000::BIGINT, 1.200, 32000000::BIGINT, 0.150),
                    (6, 2025, 9,  'ОП Волна', 620000000::BIGINT, 19, 744000000::BIGINT, 23, 125, 370000000::BIGINT, 1.650, 34000000::BIGINT, 1.200, 34000000::BIGINT, 0.165),
                    (7, 2025, 10, 'ОП Волна', 650000000::BIGINT, 20, 780000000::BIGINT, 24, 130, 390000000::BIGINT, 1.700, 35000000::BIGINT, 1.200, 35000000::BIGINT, 0.170),
                    (8, 2025, 11, 'ОП Волна', 630000000::BIGINT, 19, 756000000::BIGINT, 23, 127, 380000000::BIGINT, 1.650, 34000000::BIGINT, 1.200, 34000000::BIGINT, 0.165),
                    (9, 2025, 12, 'ОП Волна', 700000000::BIGINT, 21, 840000000::BIGINT, 25, 140, 420000000::BIGINT, 1.700, 35000000::BIGINT, 1.200, 35000000::BIGINT, 0.170),
                    (10, 2026, 1, 'ОП Волна', 720000000::BIGINT, 22, 864000000::BIGINT, 26, 145, 430000000::BIGINT, 1.800, 36000000::BIGINT, 1.200, 36000000::BIGINT, 0.175),
                    (11, 2026, 2, 'ОП Волна', 750000000::BIGINT, 23, 900000000::BIGINT, 27, 150, 450000000::BIGINT, 1.800, 37000000::BIGINT, 1.200, 37000000::BIGINT, 0.180),
                    (12, 2026, 3, 'ОП Волна', 780000000::BIGINT, 24, 936000000::BIGINT, 28, 155, 470000000::BIGINT, 1.900, 38000000::BIGINT, 1.200, 38000000::BIGINT, 0.185)
                ) AS v(id, year, month, group_name, target_revenue, target_deals, target_reserve_revenue, target_reserves, target_shows, target_pv_paid, coef_contract_from_pv, coef_avg_deal_size, coef_reserve_from_contract, coef_avg_reserve_size, coef_show_conversion)
                WHERE NOT EXISTS (SELECT 1 FROM bot.company_plans LIMIT 1);
            """)
            cur.execute("SELECT setval('bot.company_plans_id_seq', 12);")

            # manager_settings — наши синтетические МОПы
            cur.execute("""
                INSERT INTO bot.manager_settings (id, mop_id, mop_name, telegram_chat_id, is_active, receive_personal_reports)
                SELECT * FROM (VALUES
                    (1,  2544100, 'Иванов Иван',      NULL::BIGINT, TRUE, TRUE),
                    (2,  2544101, 'Петрова Мария',     NULL::BIGINT, TRUE, TRUE),
                    (3,  2544102, 'Сидоров Алексей',   NULL::BIGINT, TRUE, TRUE),
                    (4,  2544103, 'Козлова Наталья',   NULL::BIGINT, TRUE, TRUE),
                    (5,  2544104, 'Новиков Дмитрий',   NULL::BIGINT, TRUE, TRUE),
                    (6,  2544105, 'Морозова Елена',     NULL::BIGINT, TRUE, TRUE),
                    (7,  2544106, 'Волков Сергей',      NULL::BIGINT, TRUE, TRUE),
                    (8,  2544107, 'Соколова Ирина',     NULL::BIGINT, TRUE, TRUE),
                    (9,  2544108, 'Лебедев Андрей',     NULL::BIGINT, TRUE, TRUE),
                    (10, 2544109, 'Попова Анна',         NULL::BIGINT, TRUE, TRUE)
                ) AS v(id, mop_id, mop_name, telegram_chat_id, is_active, receive_personal_reports)
                WHERE NOT EXISTS (SELECT 1 FROM bot.manager_settings LIMIT 1);
            """)
            cur.execute("SELECT setval('bot.manager_settings_id_seq', 10);")

        print("✅ Bot-таблицы проверены/созданы")
    finally:
        conn.close()

def sync_table_columns(mysql_engine, pg_conn, table_name):
    """
    Проверяет колонки MySQL vs PostgreSQL и добавляет недостающие
    """
    # Получаем колонки из MySQL
    mysql_cols_query = f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'sitemanager'
    """
    mysql_cols_df = pd.read_sql(mysql_cols_query, mysql_engine)
    mysql_cols_df.columns = [c.lower() for c in mysql_cols_df.columns]
    mysql_cols = set(mysql_cols_df['column_name'].str.lower())
    
    # Получаем колонки из PostgreSQL
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = 'raw' AND table_name = '{table_name}'
    """)
    pg_cols = set(row[0].lower() for row in cursor.fetchall())
    
    # Находим недостающие колонки
    missing_cols = mysql_cols - pg_cols
    
    # Добавляем недостающие колонки
    for col in missing_cols:
        try:
            cursor.execute(f'ALTER TABLE raw."{table_name}" ADD COLUMN "{col}" TEXT')
            print(f"      ➕ Добавлена колонка: {col}")
        except Exception as e:
            print(f"      ⚠️ Не удалось добавить {col}: {e}")
    
    if missing_cols:
        pg_conn.commit()
    
    cursor.close()
    return len(missing_cols)

# ============================================================
# МЕТАДАННЫЕ ETL
# ============================================================

def ensure_metadata_table(pg_engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.etl_metadata (
        table_name TEXT PRIMARY KEY,
        last_loaded_at TIMESTAMP,
        last_max_id BIGINT,
        rows_loaded BIGINT,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    with pg_engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()

def get_last_loaded(pg_engine, table_name):
    query = """
    SELECT last_loaded_at, last_max_id 
    FROM raw.etl_metadata 
    WHERE table_name = :table_name
    """
    with pg_engine.connect() as conn:
        result = conn.execute(text(query), {'table_name': table_name}).fetchone()
    if result:
        return result[0], result[1]
    return None, None

def update_metadata(pg_engine, table_name, last_loaded_at=None, last_max_id=None, rows_loaded=0):
    query = """
    INSERT INTO raw.etl_metadata (table_name, last_loaded_at, last_max_id, rows_loaded, updated_at)
    VALUES (:table_name, :last_loaded_at, :last_max_id, :rows_loaded, NOW())
    ON CONFLICT (table_name) DO UPDATE SET
        last_loaded_at = COALESCE(:last_loaded_at, raw.etl_metadata.last_loaded_at),
        last_max_id = COALESCE(:last_max_id, raw.etl_metadata.last_max_id),
        rows_loaded = :rows_loaded,
        updated_at = NOW()
    """
    with pg_engine.connect() as conn:
        conn.execute(text(query), {
            'table_name': table_name,
            'last_loaded_at': last_loaded_at,
            'last_max_id': last_max_id,
            'rows_loaded': rows_loaded
        })
        conn.commit()

# ============================================================
# ВСТАВКА ДАННЫХ
# ============================================================

def insert_dataframe(df, table_name, pg_conn, schema='raw'):
    """Быстрая вставка через COPY"""
    if df.empty:
        return 0
    
    df_str = df.astype(str)
    df_str = df_str.replace({'nan': None, 'None': None, 'NaT': None})
    
    for col in df_str.columns:
        df_str[col] = df_str[col].apply(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
    
    buffer = StringIO()
    df_str.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    cursor = pg_conn.cursor()
    columns = ', '.join([f'"{col}"' for col in df.columns])
    copy_sql = f'COPY {schema}."{table_name}" ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
    
    cursor.copy_expert(copy_sql, buffer)
    pg_conn.commit()
    cursor.close()
    
    return len(df)

def upsert_dataframe(df, table_name, pg_conn, pg_engine):
    """UPSERT: вставка новых + обновление существующих"""
    if df.empty:
        return 0
    
    df.columns = [str(col).lower() for col in df.columns]
    pk_column = 'id' if 'id' in df.columns else 'value_id' if 'value_id' in df.columns else None
    
    if pk_column is None:
        return insert_dataframe(df, table_name, pg_conn)
    
    # Получаем существующие ID
    existing_ids_query = f'SELECT "{pk_column}" FROM raw."{table_name}"'
    with pg_engine.connect() as conn:
        existing_df = pd.read_sql(existing_ids_query, conn)
        existing_ids = set(existing_df[pk_column].astype(str))
    
    df[pk_column] = df[pk_column].astype(str)
    df_insert = df[~df[pk_column].isin(existing_ids)]
    df_update = df[df[pk_column].isin(existing_ids)]
    
    inserted = 0
    updated = 0
    
    if not df_insert.empty:
        inserted = insert_dataframe(df_insert, table_name, pg_conn)
    
    if not df_update.empty:
        cursor = pg_conn.cursor()
        columns = [col for col in df.columns if col != pk_column]
        
        for _, row in df_update.iterrows():
            set_clause = ', '.join([f'"{col}" = %s' for col in columns])
            values = [None if pd.isna(row[col]) or str(row[col]) in ('nan', 'None', 'NaT') else str(row[col]).replace('\x00', '') for col in columns]
            values.append(str(row[pk_column]))
            
            update_sql = f'UPDATE raw."{table_name}" SET {set_clause} WHERE "{pk_column}" = %s'
            cursor.execute(update_sql, values)
            updated += 1
        
        pg_conn.commit()
        cursor.close()
    
    return inserted + updated

# ============================================================
# ЗАГРУЗКА RAW ТАБЛИЦ
# ============================================================

def load_incremental(mysql_engine, pg_engine, pg_conn, table_name, config):
    """Инкрементальная загрузка по дате"""
    field = config['field']
    last_loaded, _ = get_last_loaded(pg_engine, table_name)
    
    if last_loaded is None:
        query = f"SELECT * FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name} WHERE {field} > '{last_loaded}'"
    
    df = pd.read_sql(query, mysql_engine)
    
    if df.empty:
        return 0, None
    
    rows = upsert_dataframe(df, table_name, pg_conn, pg_engine)
    df.columns = [str(col).lower() for col in df.columns]
    new_last_loaded = pd.to_datetime(df[field.lower()]).max()
    
    del df
    gc.collect()
    
    return rows, new_last_loaded

def load_incremental_chunked(mysql_engine, pg_engine, pg_conn, table_name, config):
    """Инкрементальная загрузка по дате С ЧАНКАМИ (для больших таблиц)"""
    field = config['field']
    last_loaded, _ = get_last_loaded(pg_engine, table_name)
    
    if last_loaded is None:
        query = f"SELECT * FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name} WHERE {field} > '{last_loaded}'"
    
    total_rows = 0
    new_last_loaded = None
    
    for chunk in pd.read_sql(query, mysql_engine, chunksize=CHUNK_SIZE):
        if chunk.empty:
            continue
        
        chunk.columns = [str(col).lower() for col in chunk.columns]
        
        chunk_max = pd.to_datetime(chunk[field.lower()]).max()
        if new_last_loaded is None or chunk_max > new_last_loaded:
            new_last_loaded = chunk_max
        
        rows = upsert_dataframe(chunk, table_name, pg_conn, pg_engine)
        total_rows += rows
        
        del chunk
        gc.collect()
    
    return total_rows, new_last_loaded

def load_incremental_id(mysql_engine, pg_engine, pg_conn, table_name, config):
    """Инкрементальная загрузка по ID"""
    field = config['field']
    _, last_max_id = get_last_loaded(pg_engine, table_name)
    
    if last_max_id is None:
        query = f"SELECT * FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name} WHERE {field} > {int(last_max_id)}"
    
    df = pd.read_sql(query, mysql_engine)
    
    if df.empty:
        return 0, None
    
    df.columns = [str(col).lower() for col in df.columns]
    rows = insert_dataframe(df, table_name, pg_conn)
    new_max_id = int(df[field.lower()].astype(int).max())
    
    del df
    gc.collect()
    
    return rows, new_max_id

def load_full_refresh(mysql_engine, pg_conn, table_name):
    """Полная перезагрузка (для маленьких таблиц)"""
    cursor = pg_conn.cursor()
    cursor.execute(f'TRUNCATE TABLE raw."{table_name}"')
    pg_conn.commit()
    cursor.close()
    
    df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_engine)
    
    if df.empty:
        return 0
    
    df.columns = [str(col).lower() for col in df.columns]
    rows = insert_dataframe(df, table_name, pg_conn)
    
    del df
    gc.collect()
    
    return rows

def load_full_refresh_chunked(mysql_engine, pg_conn, table_name):
    """
    Полная перезагрузка С ЧАНКАМИ и АТОМАРНОСТЬЮ
    Загружаем во временную таблицу, потом атомарно подменяем
    """
    # 0. Синхронизируем колонки (добавляем новые из MySQL)
    sync_table_columns(mysql_engine, pg_conn, table_name)
    
    temp_table = f"{table_name}_temp"
    cursor = pg_conn.cursor()
    
    # 1. Создаём временную таблицу (копия структуры)
    cursor.execute(f'DROP TABLE IF EXISTS raw."{temp_table}"')
    cursor.execute(f'CREATE TABLE raw."{temp_table}" (LIKE raw."{table_name}" INCLUDING ALL)')
    pg_conn.commit()
    
    total_rows = 0
    
    try:
        # 2. Загружаем данные чанками во временную таблицу
        for chunk in pd.read_sql(f"SELECT * FROM {table_name}", mysql_engine, chunksize=CHUNK_SIZE):
            if chunk.empty:
                continue
            
            chunk.columns = [str(col).lower() for col in chunk.columns]
            rows = insert_dataframe(chunk, temp_table, pg_conn, schema='raw')
            total_rows += rows
            
            del chunk
            gc.collect()
        
        # 3. Атомарная подмена через двойной RENAME (таблица никогда не исчезает)
        old_table = f"{table_name}_old"
        cursor.execute(f'DROP TABLE IF EXISTS raw."{old_table}"')
        cursor.execute(f'ALTER TABLE raw."{table_name}" RENAME TO "{old_table}"')
        cursor.execute(f'ALTER TABLE raw."{temp_table}" RENAME TO "{table_name}"')
        cursor.execute(f'DROP TABLE raw."{old_table}"')
        pg_conn.commit()
        
    except Exception as e:
        # Откатываем и удаляем временную таблицу
        pg_conn.rollback()
        try:
            cursor.execute(f'DROP TABLE IF EXISTS raw."{temp_table}"')
            pg_conn.commit()
        except:
            pass
        raise e
    
    finally:
        cursor.close()
    
    return total_rows

def load_raw_table(mysql_engine, pg_engine, pg_conn, table_name, config, max_retries=3):
    """Загрузка одной RAW таблицы с retry при ошибках MySQL"""
    mode = config.get('mode', 'full')
    start = time.time()
    
    for attempt in range(1, max_retries + 1):
        try:
            if mode == 'incremental':
                rows, new_date = load_incremental(mysql_engine, pg_engine, pg_conn, table_name, config)
                if new_date:
                    update_metadata(pg_engine, table_name, last_loaded_at=new_date, rows_loaded=rows)
            
            elif mode == 'incremental_chunked':
                rows, new_date = load_incremental_chunked(mysql_engine, pg_engine, pg_conn, table_name, config)
                if new_date:
                    update_metadata(pg_engine, table_name, last_loaded_at=new_date, rows_loaded=rows)
            
            elif mode == 'incremental_id':
                rows, new_max_id = load_incremental_id(mysql_engine, pg_engine, pg_conn, table_name, config)
                if new_max_id:
                    update_metadata(pg_engine, table_name, last_max_id=new_max_id, rows_loaded=rows)
            
            elif mode == 'full_chunked':
                rows = load_full_refresh_chunked(mysql_engine, pg_conn, table_name)
                update_metadata(pg_engine, table_name, rows_loaded=rows)
            
            else:  # full
                rows = load_full_refresh(mysql_engine, pg_conn, table_name)
                update_metadata(pg_engine, table_name, rows_loaded=rows)
            
            return {'status': 'ok', 'rows': rows, 'time': time.time() - start, 'mode': mode}
        
        except Exception as e:
            pg_conn.rollback()
            gc.collect()
            
            if attempt < max_retries:
                wait = attempt * 30
                print(f"      ⚠️ Попытка {attempt}/{max_retries} не удалась: {str(e)[:80]}")
                print(f"      ⏳ Повтор через {wait} сек...")
                time.sleep(wait)
                
                # Переподключаемся к MySQL
                try:
                    mysql_engine.dispose()
                except:
                    pass
            else:
                return {'status': 'error', 'rows': 0, 'time': time.time() - start, 'error': str(e), 'mode': mode}

# ============================================================
# ЗАГРУЗКА RAW СЛОЯ
# ============================================================

def load_raw_layer():
    print("\n📥 ЭТАП 1: Загрузка RAW слоя")
    print("-" * 50)
    
    mysql_engine = get_mysql_engine()
    pg_engine = get_pg_engine()
    pg_conn = get_pg_connection()
    
    ensure_metadata_table(pg_engine)
    
    tables_sorted = sorted(TABLES_CONFIG.items(), key=lambda x: x[1].get('order', 99))
    
    results = []
    errors = []
    start = time.time()
    
    for table_name, config in tables_sorted:
        mode = config.get('mode', 'full')
        mode_icon = {
            'incremental': '🔄', 
            'incremental_chunked': '🔄📦',
            'incremental_id': '🆔', 
            'full': '📥',
            'full_chunked': '📥🔒'  # 🔒 = атомарная
        }
        
        result = load_raw_table(mysql_engine, pg_engine, pg_conn, table_name, config)
        result['table'] = table_name
        results.append(result)
        
        if result['status'] == 'ok':
            print(f"   {mode_icon.get(mode, '?')} {table_name}: {result['rows']:,} строк ({result['time']:.1f}s)")
        else:
            error_msg = result.get('error', '')[:50]
            print(f"   ❌ {table_name}: {error_msg}")
            errors.append(f"{table_name}: {error_msg}")
    
    pg_conn.close()
    
    total_time = time.time() - start
    ok_count = sum(1 for r in results if r['status'] == 'ok')
    error_count = sum(1 for r in results if r['status'] == 'error')
    total_rows = sum(r['rows'] for r in results)
    
    print(f"\n   📊 RAW итого: {ok_count} ок, {error_count} ошибок, {total_rows:,} строк за {total_time:.1f}s")
    
    gc.collect()
    
    return error_count == 0, errors

# ============================================================
# ВЫПОЛНЕНИЕ SQL-СКРИПТОВ С ТАЙМАУТОМ
# ============================================================

def execute_sql_with_timeout(pg_conn, sql, timeout_seconds):
    """
    Выполняет SQL с таймаутом.
    Возвращает (success, error_message)
    """
    result = {'success': False, 'error': None}
    
    def execute():
        try:
            cursor = pg_conn.cursor()
            cursor.execute(sql)
            pg_conn.commit()
            cursor.close()
            result['success'] = True
        except Exception as e:
            result['error'] = str(e)
    
    thread = threading.Thread(target=execute)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        try:
            pg_conn.cancel()
            pg_conn.rollback()
        except:
            pass
        return False, f"TIMEOUT ({timeout_seconds}s) - возможно блокировка таблицы"
    
    if result['success']:
        return True, None
    else:
        try:
            pg_conn.rollback()
        except:
            pass
        return False, result['error']


def run_sql_scripts():
    print("\n🔄 ЭТАП 2: Обновление staging → mart → bot")
    print("-" * 50)
    
    pg_conn = get_pg_connection()
    start = time.time()
    success_count = 0
    error_count = 0
    errors = []
    timeout_scripts = []
    
    for script_name in SQL_SCRIPTS:
        script_path = SQL_DIR / script_name
        script_start = time.time()
        
        if not script_path.exists():
            print(f"   ⚠️ Файл не найден: {script_name}")
            error_count += 1
            errors.append(f"Файл не найден: {script_name}")
            continue
        if script_name in SQL_DEPENDENCIES:
            missing = []
            for dep_table in SQL_DEPENDENCIES[script_name]:
                schema, table = dep_table.split('.')
                cursor = pg_conn.cursor()
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
                    (schema, table)
                )
                exists = cursor.fetchone()[0]
                cursor.close()
                if not exists:
                    missing.append(dep_table)
            if missing:
                print(f"   ⏭️ {script_name}: SKIP (нет источника: {', '.join(missing)})")
                continue
        
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                sql = f.read()
            
            success, error_msg = execute_sql_with_timeout(pg_conn, sql, SQL_SCRIPT_TIMEOUT)
            elapsed = time.time() - script_start
            
            if success:
                print(f"   ✅ {script_name} ({elapsed:.1f}s)")
                success_count += 1
            else:
                if "TIMEOUT" in str(error_msg):
                    print(f"   ⏱️ {script_name}: TIMEOUT ({elapsed:.1f}s)")
                    timeout_scripts.append(script_name)
                else:
                    print(f"   ❌ {script_name}: {str(error_msg)[:50]}")
                error_count += 1
                errors.append(f"{script_name}: {str(error_msg)[:50]}")
        
        except Exception as e:
            pg_conn.rollback()
            error_msg = str(e)[:50]
            print(f"   ❌ {script_name}: {error_msg}")
            error_count += 1
            errors.append(f"{script_name}: {error_msg}")
    
    pg_conn.close()
    
    total_time = time.time() - start
    print(f"\n   📊 SQL итого: {success_count} ок, {error_count} ошибок за {total_time:.1f}s")
    
    if timeout_scripts:
        alert_message = (
            f"⏱️ <b>ETL TIMEOUT</b>\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"Скрипты превысили таймаут ({SQL_SCRIPT_TIMEOUT}s):\n"
            f"• " + "\n• ".join(timeout_scripts) + "\n\n"
            f"Возможная причина: блокировка таблицы (бот или другой процесс)"
        )
        send_telegram_alert(alert_message)
    
    return error_count == 0, errors

# ============================================================
# ПОЛНЫЙ ЦИКЛ ETL
# ============================================================

def run_full_etl():
    print("\n" + "=" * 60)
    print(f"🚀 ETL ЗАПУСК: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start = time.time()  # ← это было потеряно

    # Этап 1: RAW из MySQL (Bitrix24) — отключён в demo-режиме
    # RAW-данные загружаются через generate_fake_data.py
    # raw_ok, raw_errors = load_raw_layer()

    # Этап 1.5: Excel Tager — отключён в demo-режиме
    # from load_tager_fg import load_fg_tables; load_fg_tables()

    # Этап 1.6: Excel Nextcloud — отключён в demo-режиме
    # from load_soprovozhdenie import load_soprovozhdenie; load_soprovozhdenie()

    # Этап 1.7: API bnMAP — отключён в demo-режиме
    # from load_cmap_deals import run as run_cmap_deals; run_cmap_deals()

    raw_ok = True
    all_errors = []

    # Этап 2: SQL staging → mart → bot
    sql_ok, sql_errors = run_sql_scripts()
    all_errors.extend(sql_errors)

    total_time = time.time() - start
    status = "✅ УСПЕШНО" if (raw_ok and sql_ok) else "⚠️ С ОШИБКАМИ"

    print("\n" + "=" * 60)
    print(f"🏁 ETL ЗАВЕРШЁН: {status} за {total_time:.1f} сек")
    print("=" * 60)

    gc.collect()

    non_timeout_errors = [e for e in all_errors if "TIMEOUT" not in e]
    if non_timeout_errors:
        error_list = "\n".join([f"• {e}" for e in non_timeout_errors[:5]])
        if len(non_timeout_errors) > 5:
            error_list += f"\n... и ещё {len(non_timeout_errors) - 5} ошибок"

        alert_message = (
            f"🚨 <b>ETL ОШИБКА</b>\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱️ Время: {total_time:.1f} сек\n\n"
            f"<b>Ошибки:</b>\n{error_list}"
        )
        send_telegram_alert(alert_message)
# ============================================================
# ЗАПУСК
# ============================================================

def main():
    print("🔧 ETL v3 запущен (чанки + атомарность)")
    print(f"⏰ Интервал: каждые 10 минут")
    print(f"📁 SQL папка: {SQL_DIR}")
    print(f"📢 Алерты: {len(ALERT_CHAT_IDS)} получателей")
    print(f"⏱️ Таймаут SQL-скрипта: {SQL_SCRIPT_TIMEOUT} сек")
    print(f"📦 Размер чанка: {CHUNK_SIZE:,} строк")
    print(f"🔒 Атомарная загрузка: full_chunked таблицы")


    # Создаём схемы если их нет
    ensure_schemas()
    
    # Создаём bot-таблицы если их нет
    ensure_bot_tables()
    
    # Запускаем сразу
    run_full_etl()
    
    # Планируем каждые 10 минут
    schedule.every(10).minutes.do(run_full_etl)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        alert_message = (
            f"⚠️ <b>ETL ОСТАНОВЛЕН</b>\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Причина: Ручная остановка (Ctrl+C)"
        )
        send_telegram_alert(alert_message)
        print("\n\n⚠️ ETL остановлен. Алерт отправлен в Telegram.")

if __name__ == "__main__":
    main()
