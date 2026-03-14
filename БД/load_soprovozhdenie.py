# load_soprovozhdenie.py
# Загрузка таблиц Отдела сопровождения из Excel в raw слой
# Источник: Nextcloud (Excel файл, 2 листа)
#
# Логика:
# - Лист "ПОДПИСАННЫЕ" → raw.raw_podpisannye
# - Лист "В работе" → raw.raw_v_rabote
# - Полная перезагрузка (DROP + CREATE) при каждом запуске
# - Все поля загружаются как TEXT

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
from datetime import datetime
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

# Путь к файлу
SOURCE_FILE = Path(r"C:\Users\n.storozhev\Nextcloud\Документы Отдела сопровождения\брони_в_работе.xlsx")

# Маппинг: название листа → целевая таблица
SHEETS_CONFIG = {
    "ПОДПИСАННЫЕ": {"schema": "raw", "table": "raw_podpisannye"},
    "В работе":    {"schema": "raw", "table": "raw_v_rabote"},
}

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
# ЧТЕНИЕ EXCEL ФАЙЛА
# ============================================================

def read_sheet(file_path: Path, sheet_name: str) -> pd.DataFrame:
    """Читает один лист Excel файла"""
    print(f"   📖 Читаю лист: «{sheet_name}»")
    
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=0,
        engine='openpyxl'
    )
    
    # Добавляем метаданные
    df['_source_file'] = file_path.name
    df['_source_sheet'] = sheet_name
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
        col_clean = col_clean.replace('№', 'num')
        
        while '__' in col_clean:
            col_clean = col_clean.replace('__', '_')
        
        col_clean = col_clean.strip('_')
        
        if not col_clean or col_clean.isdigit():
            col_clean = f"col_{len(new_columns)}"
        
        # Дедупликация имён колонок
        base_name = col_clean
        counter = 1
        while col_clean in new_columns:
            col_clean = f"{base_name}_{counter}"
            counter += 1
        
        new_columns.append(col_clean)
    
    df.columns = new_columns
    return df

# ============================================================
# ЗАГРУЗКА В БД
# ============================================================

def create_table_from_df(pg_conn, df: pd.DataFrame, schema: str, table: str):
    """Создаёт таблицу на основе DataFrame (DROP + CREATE)"""
    cursor = pg_conn.cursor()
    
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    
    columns_ddl = []
    for col in df.columns:
        columns_ddl.append(f'"{col}" TEXT')
    
    ddl = f"""
    CREATE TABLE {schema}.{table} (
        {', '.join(columns_ddl)}
    )
    """
    
    cursor.execute(ddl)
    pg_conn.commit()
    cursor.close()
    print(f"   🏗️ Таблица {schema}.{table} создана ({len(df.columns)} колонок)")

def insert_dataframe(df: pd.DataFrame, pg_conn, schema: str, table: str):
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
    copy_sql = f'COPY {schema}.{table} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
    
    cursor.copy_expert(copy_sql, buffer)
    pg_conn.commit()
    cursor.close()
    
    return len(df)

# ============================================================
# ЗАГРУЗКА ОДНОГО ЛИСТА
# ============================================================

def load_sheet(pg_conn, file_path: Path, sheet_name: str, schema: str, table: str):
    """Загружает один лист в указанную таблицу"""
    
    df = read_sheet(file_path, sheet_name)
    df = normalize_columns(df)
    create_table_from_df(pg_conn, df, schema, table)
    rows_inserted = insert_dataframe(df, pg_conn, schema, table)
    print(f"   ✅ {schema}.{table}: загружено {rows_inserted} строк")
    
    return rows_inserted

# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def load_soprovozhdenie():
    """Основная функция загрузки таблиц Отдела сопровождения"""
    print("\n" + "=" * 60)
    print(f"🚀 ЗАГРУЗКА СОПРОВОЖДЕНИЕ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"📁 Файл: {SOURCE_FILE}")
    print()
    
    try:
        # Проверяем наличие файла
        if not SOURCE_FILE.exists():
            print(f"   ❌ Файл не найден: {SOURCE_FILE}")
            return False
        
        pg_conn = get_pg_connection()
        
        total_rows = 0
        for sheet_name, config in SHEETS_CONFIG.items():
            print(f"\n   --- Лист: «{sheet_name}» → {config['schema']}.{config['table']} ---")
            rows = load_sheet(
                pg_conn, 
                SOURCE_FILE, 
                sheet_name, 
                config['schema'], 
                config['table']
            )
            total_rows += rows
        
        pg_conn.close()
        
        print("\n" + "=" * 60)
        print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: {total_rows} строк в {len(SHEETS_CONFIG)} таблиц")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        return False

# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":
    load_soprovozhdenie()
