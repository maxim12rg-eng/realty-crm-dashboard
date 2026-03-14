# load_all_tables_fast.py
# Быстрая загрузка таблиц Bitrix24 → PostgreSQL с методом COPY
# Full Refresh режим (полная перезагрузка)
# v2: Все колонки TEXT + ROLLBACK после ошибок

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
import time
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

# Загружаем .env
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

# Список таблиц для загрузки
TABLES_TO_LOAD = [
    # Маленькие таблицы (< 10k строк) — сначала
    'b_crm_deal_category',
    'b_iblock',
    'b_crm_dynamic_items_184',
    'b_crm_company',
    'b_crm_dynamic_items_164',
    'b_crm_status',
    'b_crm_dynamic_items_1054',
    'b_user',
    'b_iblock_fields',
    'b_user_field',
    'b_user_field_enum',
    'b_crm_dynamic_items_186',
    
    # Средние таблицы (10k - 100k строк)
    'b_crm_contact',
    'b_uts_crm_contact',
    'b_crm_deal',
    'b_uts_crm_deal',
    'b_crm_lead',
    'b_uts_crm_lead',
    'b_crm_utm',
    'b_crm_field_multi',
    'b_crm_lead_status_history',
    
    # Большие таблицы (100k+ строк) — в конце
    'b_iblock_element',
    'b_crm_deal_stage_history',
    'b_crm_act',
    'b_voximplant_statistic',
    
    # Очень большие таблицы (1M+ строк) — самые последние
    'b_crm_timeline',
    'b_crm_timeline_bind',
    'b_iblock_element_property',
]

# Настройки
CHUNK_SIZE = 50000  # Размер чанка для чтения из MySQL
TIMEOUT_MINUTES = 10  # Таймаут на одну таблицу

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
    """Подключение к PostgreSQL через psycopg2 (для COPY)"""
    return psycopg2.connect(
        host=os.getenv('PG_HOST'),
        port=int(os.getenv('PG_PORT')),
        database=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD')
    )

def get_pg_engine():
    """Подключение к PostgreSQL через SQLAlchemy (для запросов)"""
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

# ============================================================
# ФУНКЦИЯ COPY (быстрая вставка)
# ============================================================

def copy_dataframe_to_postgres(df, table_name, pg_conn):
    """
    Быстрая загрузка DataFrame в PostgreSQL через COPY
    В 10-50 раз быстрее чем INSERT
    """
    # Конвертируем всё в строки (безопасно для COPY)
    df_str = df.astype(str)
    df_str = df_str.replace({'nan': None, 'None': None, 'NaT': None})
    
    # Убираем NULL-байты (\x00) — PostgreSQL их не принимает
    for col in df_str.columns:
        df_str[col] = df_str[col].apply(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
    
    # Создаём буфер с CSV-данными
    buffer = StringIO()
    df_str.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    cursor = pg_conn.cursor()
    
    # Получаем список колонок
    columns = ', '.join([f'"{col}"' for col in df.columns])
    
    # COPY команда
    copy_sql = f'COPY raw."{table_name}" ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
    
    cursor.copy_expert(copy_sql, buffer)
    pg_conn.commit()
    cursor.close()

# ============================================================
# СОЗДАНИЕ ТАБЛИЦЫ (ВСЕ КОЛОНКИ TEXT)
# ============================================================

def create_table_from_dataframe(df, table_name, pg_engine):
    """
    Создаёт пустую таблицу в PostgreSQL
    ВСЕ колонки — TEXT (безопасно, типизация в staging)
    + индекс на id/value_id для быстрого upsert в etl_full_v6
    """
    # Все колонки делаем TEXT — это безопасно для RAW слоя
    columns_ddl = [f'"{col}" TEXT' for col in df.columns]
    columns_str = ',\n    '.join(columns_ddl)
    
    # Определяем колонку для индекса
    cols_lower = [str(col).lower() for col in df.columns]
    if 'id' in cols_lower:
        index_col = 'id'
    elif 'value_id' in cols_lower:
        index_col = 'value_id'
    else:
        index_col = None
    
    ddl = f'''
    DROP TABLE IF EXISTS raw."{table_name}" CASCADE;
    CREATE TABLE raw."{table_name}" (
        {columns_str}
    );
    '''
    
    # Добавляем CREATE INDEX если есть колонка для индекса
    if index_col:
        ddl += f'''
    CREATE INDEX idx_{table_name}_{index_col} ON raw."{table_name}" ("{index_col}");
    '''
    
    with pg_engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()

# ============================================================
# ЗАГРУЗКА ОДНОЙ ТАБЛИЦЫ
# ============================================================

def load_table(mysql_engine, pg_engine, pg_conn, table_name, max_retries=3):
    """Загружает одну таблицу с методом COPY и retry при ошибках"""
    
    start_time = time.time()
    
    for attempt in range(1, max_retries + 1):
        try:
            # 1. Получаем количество строк
            with mysql_engine.connect() as conn:
                result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn)
                total_rows = result['cnt'][0]
            
            if total_rows == 0:
                return {'status': 'empty', 'rows': 0, 'time': time.time() - start_time}
            
            # 2. Читаем первый чанк для создания структуры
            first_chunk = pd.read_sql(
                f"SELECT * FROM {table_name} LIMIT 1",
                mysql_engine
            )
            first_chunk.columns = [str(col).lower() for col in first_chunk.columns]
            
            # 3. Создаём таблицу (все колонки TEXT)
            create_table_from_dataframe(first_chunk, table_name, pg_engine)
            
            # 4. Загружаем данные чанками
            loaded_rows = 0
            chunk_num = 0
            total_chunks = (total_rows // CHUNK_SIZE) + 1
            
            for offset in range(0, total_rows, CHUNK_SIZE):
                chunk_num += 1
                
                # Проверка таймаута
                elapsed = time.time() - start_time
                if elapsed > TIMEOUT_MINUTES * 60:
                    return {
                        'status': 'timeout',
                        'rows': loaded_rows,
                        'time': elapsed,
                        'error': f'Таймаут {TIMEOUT_MINUTES} мин'
                    }
                
                # Читаем чанк из MySQL
                query = f"SELECT * FROM {table_name} LIMIT {CHUNK_SIZE} OFFSET {offset}"
                df_chunk = pd.read_sql(query, mysql_engine)
                
                if df_chunk.empty:
                    break
                
                # Приводим колонки к нижнему регистру
                df_chunk.columns = [str(col).lower() for col in df_chunk.columns]
                
                # COPY в PostgreSQL (быстро!)
                copy_dataframe_to_postgres(df_chunk, table_name, pg_conn)
                
                loaded_rows += len(df_chunk)
                
                # Прогресс для больших таблиц
                if total_chunks > 1:
                    progress = (loaded_rows / total_rows) * 100
                    speed = loaded_rows / (time.time() - start_time)
                    print(f"      Чанк {chunk_num}/{total_chunks}: {loaded_rows:,}/{total_rows:,} ({progress:.0f}%) | {speed:.0f} строк/сек")
            
            return {
                'status': 'ok',
                'rows': loaded_rows,
                'time': time.time() - start_time
            }
        
        except Exception as e:
            try:
                pg_conn.rollback()
            except:
                pass
            
            if attempt < max_retries:
                wait = attempt * 30
                print(f"      ⚠️ Попытка {attempt}/{max_retries} не удалась: {str(e)[:80]}")
                print(f"      ⏳ Повтор через {wait} сек...")
                time.sleep(wait)
                
                try:
                    mysql_engine.dispose()
                except:
                    pass
            else:
                return {
                    'status': 'error',
                    'rows': 0,
                    'time': time.time() - start_time,
                    'error': str(e)
                }
# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def main():
    print("=" * 70)
    print("🚀 БЫСТРАЯ ЗАГРУЗКА BITRIX24 → PostgreSQL (метод COPY)")
    print(f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 Таблиц: {len(TABLES_TO_LOAD)}")
    print(f"📦 Чанк: {CHUNK_SIZE:,} строк")
    print(f"💡 Все колонки загружаются как TEXT (типизация в staging)")
    print("=" * 70)

    ensure_schemas()
    
    # Подключения
    print("\n📡 Подключаюсь к базам...")
    mysql_engine = get_mysql_engine()
    pg_engine = get_pg_engine()
    pg_conn = get_pg_connection()
    print("   ✅ Подключения установлены")
    
    # Результаты
    results = []
    total_start = time.time()
    
    print("\n" + "-" * 70)
    
    # Загрузка таблиц
    for i, table_name in enumerate(TABLES_TO_LOAD, 1):
        print(f"\n[{i}/{len(TABLES_TO_LOAD)}] 📥 {table_name}")
        
        result = load_table(mysql_engine, pg_engine, pg_conn, table_name)
        result['table'] = table_name
        results.append(result)
        
        # Вывод статуса
        if result['status'] == 'ok':
            speed = result['rows'] / result['time'] if result['time'] > 0 else 0
            print(f"      ✅ {result['rows']:,} строк за {result['time']:.1f} сек ({speed:.0f} строк/сек)")
        elif result['status'] == 'empty':
            print(f"      ⚠️  Пустая таблица")
        elif result['status'] == 'timeout':
            print(f"      ⏱️  Таймаут: загружено {result['rows']:,} строк")
        else:
            print(f"      ❌ Ошибка: {result.get('error', 'Unknown')[:100]}")
    
    # Закрываем соединение
    pg_conn.close()
    
    # ============================================================
    # ИТОГОВЫЙ ОТЧЁТ
    # ============================================================
    
    total_time = time.time() - total_start
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    
    ok_count = sum(1 for r in results if r['status'] == 'ok')
    empty_count = sum(1 for r in results if r['status'] == 'empty')
    error_count = sum(1 for r in results if r['status'] == 'error')
    timeout_count = sum(1 for r in results if r['status'] == 'timeout')
    total_rows = sum(r['rows'] for r in results)
    
    print("\n" + "=" * 70)
    print("📊 ИТОГОВЫЙ ОТЧЁТ")
    print("=" * 70)
    print(f"⏱️  Общее время: {minutes} мин {seconds} сек")
    print(f"✅ Успешно: {ok_count} таблиц")
    print(f"⚠️  Пустых: {empty_count}")
    print(f"⏱️  Таймаут: {timeout_count}")
    print(f"❌ Ошибок: {error_count}")
    print(f"📈 Всего строк: {total_rows:,}")
    
    avg_speed = total_rows / total_time if total_time > 0 else 0
    print(f"🚀 Средняя скорость: {avg_speed:.0f} строк/сек")
    
    # Детальная таблица
    print("\n📋 ДЕТАЛИ:")
    print("-" * 70)
    print(f"{'Таблица':<35} {'Статус':<8} {'Строк':>12} {'Время':>8} {'Скорость':>10}")
    print("-" * 70)
    
    for r in results:
        status_icon = {'ok': '✅', 'empty': '⚠️', 'error': '❌', 'timeout': '⏱️'}.get(r['status'], '?')
        rows_str = f"{r['rows']:,}" if r['rows'] > 0 else '-'
        time_str = f"{r['time']:.1f}s"
        speed = r['rows'] / r['time'] if r['time'] > 0 and r['rows'] > 0 else 0
        speed_str = f"{speed:.0f}/s" if speed > 0 else '-'
        print(f"{r['table']:<35} {status_icon:<8} {rows_str:>12} {time_str:>8} {speed_str:>10}")
    
    print("-" * 70)
    
    # Ошибки
    errors = [r for r in results if r['status'] in ('error', 'timeout')]
    if errors:
        print("\n⚠️  ПРОБЛЕМЫ:")
        for r in errors:
            print(f"   {r['table']}: {r.get('error', r['status'])[:80]}")
    
    # Проверка в PostgreSQL
    print("\n🔍 ПРОВЕРКА В POSTGRESQL:")
    with pg_engine.connect() as conn:
        check = pd.read_sql("""
            SELECT table_name, 
                   pg_size_pretty(pg_total_relation_size('raw.' || quote_ident(table_name))) as size
            FROM information_schema.tables 
            WHERE table_schema = 'raw'
            ORDER BY pg_total_relation_size('raw.' || quote_ident(table_name)) DESC
        """, conn)
        
        print(f"   Таблиц в raw: {len(check)}")
        for _, row in check.head(10).iterrows():
            print(f"   • {row['table_name']}: {row['size']}")
        if len(check) > 10:
            print(f"   ... и ещё {len(check) - 10} таблиц")
    
    print("\n" + "=" * 70)
    print("✅ ЗАГРУЗКА ЗАВЕРШЕНА!")
    print("=" * 70)

if __name__ == "__main__":
    main()
