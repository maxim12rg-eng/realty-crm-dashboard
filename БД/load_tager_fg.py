# load_tager_fg.py
# Загрузка таблиц FG (контроль качества показов) из Excel в raw слой
# Источник: Tager (Excel файлы)
# 
# Логика:
# - Историческая таблица (.xlsm) — загружается только если raw_fg пустая
# - Новая таблица (.xlsx) — перезаписывает данные за последние 3 дня

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
from datetime import datetime, timedelta
import os
from pathlib import Path
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=FutureWarning)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

# Путь к папке с файлами Tager
TAGER_FOLDER = Path(r"C:\Users\m.lobanov\Отдел Контроля Качества\Общие документы ОКК")

# Файлы для загрузки
HISTORICAL_FILE = ("Отчетная таблица КК показы Nedvex", ".xlsm")
INCREMENTAL_FILE = ("Новая отчетная таблица КК показы Nedvex", ".xlsx")

# Параметры чтения Excel
HEADER_ROW = 5  # Заголовки в 6-й строке (индекс 5)
USE_COLS = "A:BG"  # Колонки от A до BG

# Целевая таблица
TARGET_SCHEMA = "raw"
TARGET_TABLE = "raw_fg"

# Сколько дней перезаписывать
DAYS_TO_REFRESH = 3

# ============================================================
# ПОДКЛЮЧЕНИЕ К БД
# ============================================================

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

# ============================================================
# ЧТЕНИЕ EXCEL ФАЙЛОВ
# ============================================================

def read_tager_file(file_path: Path) -> pd.DataFrame:
    """Читает один Excel файл из Tager"""
    print(f"   📖 Читаю: {file_path.name}")
    
    df = pd.read_excel(
        file_path,
        header=HEADER_ROW,
        usecols=USE_COLS,
        engine='openpyxl'
    )
    
    # Добавляем метаданные
    df['_source_file'] = file_path.name
    df['_loaded_at'] = datetime.now()
    
    print(f"      → {len(df)} строк, {len(df.columns)} колонок")
    return df

# ============================================================
# НОРМАЛИЗАЦИЯ КОЛОНОК
# ============================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит названия колонок к безопасному виду для PostgreSQL"""
    
    new_columns = []
    for col in df.columns:
        col_clean = str(col).strip().lower()
        col_clean = col_clean.replace(' ', '_')
        col_clean = col_clean.replace('.', '_')
        col_clean = col_clean.replace('/', '_')
        col_clean = col_clean.replace('(', '')
        col_clean = col_clean.replace(')', '')
        col_clean = col_clean.replace('?', '')
        col_clean = col_clean.replace(',', '')
        col_clean = col_clean.replace('-', '_')
        col_clean = col_clean.replace('\n', '_')
        
        while '__' in col_clean:
            col_clean = col_clean.replace('__', '_')
        
        col_clean = col_clean.strip('_')
        
        if not col_clean or col_clean.isdigit():
            col_clean = f"col_{len(new_columns)}"
        
        new_columns.append(col_clean)
    
    df.columns = new_columns
    return df

# ============================================================
# ПРОВЕРКИ БД
# ============================================================

def table_exists(pg_conn) -> bool:
    """Проверяет существует ли таблица"""
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{TARGET_SCHEMA}' 
            AND table_name = '{TARGET_TABLE}'
        )
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists

def table_is_empty(pg_conn) -> bool:
    """Проверяет пустая ли таблица"""
    cursor = pg_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count == 0

# ============================================================
# ЗАГРУЗКА В БД
# ============================================================

def create_table_from_df(pg_conn, df: pd.DataFrame):
    """Создаёт таблицу на основе DataFrame (DROP + CREATE)"""
    cursor = pg_conn.cursor()
    
    cursor.execute(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{TARGET_TABLE}")
    
    columns_ddl = []
    for col in df.columns:
        columns_ddl.append(f'"{col}" TEXT')
    
    ddl = f"""
    CREATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} (
        {', '.join(columns_ddl)}
    )
    """
    
    cursor.execute(ddl)
    pg_conn.commit()
    cursor.close()
    print(f"   🏗️ Таблица {TARGET_SCHEMA}.{TARGET_TABLE} создана")

def insert_dataframe(df: pd.DataFrame, pg_conn):
    """Быстрая вставка через COPY"""
    if df.empty:
        return 0
    
    df_str = df.astype(str)
    df_str = df_str.replace({'nan': None, 'None': None, 'NaT': None})
    
    for col in df_str.columns:
        df_str[col] = df_str[col].apply(
            lambda x: x.replace('\\', '\\\\').replace('\t', ' ').replace('\n', ' ').replace('\r', ' ') 
            if x and isinstance(x, str) else x
        )
    
    cursor = pg_conn.cursor()
    
    buffer = StringIO()
    df_str.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    columns = ', '.join([f'"{col}"' for col in df_str.columns])
    copy_sql = f'COPY {TARGET_SCHEMA}.{TARGET_TABLE} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
    
    cursor.copy_expert(copy_sql, buffer)
    pg_conn.commit()
    cursor.close()
    
    return len(df)

def delete_recent_days(pg_conn, days: int):
    """Удаляет данные за последние N дней"""
    cursor = pg_conn.cursor()
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # Удаляем по дате (поле дата в формате YYYY-MM-DD)
    cursor.execute(f"""
        DELETE FROM {TARGET_SCHEMA}.{TARGET_TABLE}
        WHERE дата >= '{cutoff_date}'
    """)
    deleted = cursor.rowcount
    pg_conn.commit()
    cursor.close()
    
    print(f"   🗑️ Удалено {deleted} строк за последние {days} дней (с {cutoff_date})")
    return deleted

# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def load_fg_tables():
    """Основная функция загрузки FG таблиц"""
    print("\n" + "=" * 60)
    print(f"🚀 ЗАГРУЗКА FG ТАБЛИЦ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"📁 Папка: {TAGER_FOLDER}")
    print()
    
    try:
        pg_conn = get_pg_connection()
        
        # Проверяем состояние таблицы
        needs_full_load = not table_exists(pg_conn) or table_is_empty(pg_conn)
        
        if needs_full_load:
            # === ПОЛНАЯ ЗАГРУЗКА ===
            print("📥 Режим: ПОЛНАЯ ЗАГРУЗКА (таблица пустая или не существует)")
            print()
            
            all_dfs = []
            
            # Читаем историческую таблицу
            hist_path = TAGER_FOLDER / f"{HISTORICAL_FILE[0]}{HISTORICAL_FILE[1]}"
            if hist_path.exists():
                df_hist = read_tager_file(hist_path)
                all_dfs.append(df_hist)
            else:
                print(f"   ⚠️ Историческая таблица не найдена: {hist_path}")
            
            # Читаем новую таблицу
            incr_path = TAGER_FOLDER / f"{INCREMENTAL_FILE[0]}{INCREMENTAL_FILE[1]}"
            if incr_path.exists():
                df_incr = read_tager_file(incr_path)
                all_dfs.append(df_incr)
            else:
                print(f"   ⚠️ Новая таблица не найдена: {incr_path}")
            
            if not all_dfs:
                raise FileNotFoundError("Не найдено ни одного файла для загрузки")
            
            df = pd.concat(all_dfs, ignore_index=True)
            print(f"\n   📊 Всего после union: {len(df)} строк")
            
            df = normalize_columns(df)
            create_table_from_df(pg_conn, df)
            rows_inserted = insert_dataframe(df, pg_conn)
            print(f"   ✅ Загружено: {rows_inserted} строк")
            
        else:
            # === ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА ===
            print(f"🔄 Режим: ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА (обновление за {DAYS_TO_REFRESH} дня)")
            print()
            
            # Читаем только новую таблицу
            incr_path = TAGER_FOLDER / f"{INCREMENTAL_FILE[0]}{INCREMENTAL_FILE[1]}"
            if not incr_path.exists():
                print(f"   ⚠️ Новая таблица не найдена: {incr_path}")
                pg_conn.close()
                return True
            
            df = read_tager_file(incr_path)
            df = normalize_columns(df)
            
            # Фильтруем только последние N дней
            cutoff_date = (datetime.now() - timedelta(days=DAYS_TO_REFRESH)).strftime('%Y-%m-%d')
            df_recent = df[df['дата'] >= cutoff_date].copy()
            print(f"   📊 Строк за последние {DAYS_TO_REFRESH} дня: {len(df_recent)}")
            
            if df_recent.empty:
                print("   ℹ️ Нет новых данных для загрузки")
                pg_conn.close()
                return True
            
            # Удаляем старые данные за эти дни
            delete_recent_days(pg_conn, DAYS_TO_REFRESH)
            
            # Вставляем новые
            rows_inserted = insert_dataframe(df_recent, pg_conn)
            print(f"   ✅ Загружено: {rows_inserted} строк")
        
        pg_conn.close()
        
        print("\n" + "=" * 60)
        print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        return False

# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":
    load_fg_tables()
