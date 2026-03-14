# load_cmap.py
# Загрузка данных из CMAP API (bndev.it) → raw слой PostgreSQL
# Источник: bnMAP.pro API
#   - pbi-full-deals → raw.raw_cmap_deals (с пагинацией, 10 потоков)
#   - pbi-prices     → raw.raw_cmap_prices (без пагинации, один запрос)
# Режим: Full Refresh, фильтр сделок от 2020
# Все поля TEXT

import pandas as pd
import psycopg2
from io import StringIO
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

CMAP_API_URL = "https://krnd.bndev.it/cmap/analytics.json"
CMAP_PBI_TOKEN = os.getenv("CMAP_PBI_TOKEN")
REQUEST_TIMEOUT = 120
PRICES_TIMEOUT = 300  # прайсы могут быть большими
MAX_WORKERS = 10
YEAR_FROM = 2020

BOT_TOKEN = os.getenv("ETL_BOT_TOKEN")
ALERT_CHAT_IDS = [612837092, 890703314]

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

# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in ALERT_CHAT_IDS:
        try:
            requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=10)
        except Exception:
            pass

# ============================================================
# ЗАГРУЗКА В POSTGRESQL (FULL REFRESH)
# ============================================================

def load_to_postgres(df: pd.DataFrame, schema: str, table: str):
    """Full refresh: DROP → CREATE → COPY"""
    pg_conn = get_pg_connection()
    cursor = pg_conn.cursor()

    try:
        full_table = f'{schema}."{table}"'
        tmp_table = f'{schema}."{table}_tmp"'

        # Нормализуем колонки
        df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]

        # Всё в TEXT
        df_text = df.astype(str).replace({'nan': None, 'None': None, 'NaT': None, 'none': None})

        # Создаём временную таблицу
        columns_ddl = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f'DROP TABLE IF EXISTS {tmp_table}')
        cursor.execute(f'CREATE TABLE {tmp_table} ({columns_ddl})')

        # COPY
        buffer = StringIO()
        df_text.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        columns_list = ', '.join([f'"{col}"' for col in df.columns])
        cursor.copy_expert(
            f"COPY {tmp_table} ({columns_list}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
            buffer
        )

        # Атомарная подмена
        cursor.execute(f'DROP TABLE IF EXISTS {full_table}')
        cursor.execute(f'ALTER TABLE {tmp_table} RENAME TO "{table}"')

        # Метаданные
        try:
            cursor.execute(f"""
                INSERT INTO raw.etl_metadata (table_name, last_loaded_at, rows_loaded, updated_at)
                VALUES ('{table}', NOW(), {len(df)}, NOW())
                ON CONFLICT (table_name) DO UPDATE SET
                    last_loaded_at = NOW(), rows_loaded = {len(df)}, updated_at = NOW()
            """)
        except Exception:
            pass

        pg_conn.commit()
        print(f"   ✅ Загружено {len(df)} строк в {full_table}")

    except Exception as e:
        pg_conn.rollback()
        raise e
    finally:
        cursor.close()
        pg_conn.close()

# ============================================================
# 1. СДЕЛКИ (pbi-full-deals) — с пагинацией, 10 потоков
# ============================================================

def _fetch_deals_page(page: int) -> list:
    resp = requests.get(CMAP_API_URL, params={
        "act": "pbi-full-deals",
        "pbi": CMAP_PBI_TOKEN,
        "page": page
    }, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "ok":
        raise ValueError(f"API error на стр. {page}: {body}")
    return body["content"]["data"]


def fetch_deals() -> pd.DataFrame:
    """Загружает все сделки из CMAP API (10 потоков)"""
    print(f"   🌐 Сделки: запрос страницы 1...")
    resp = requests.get(CMAP_API_URL, params={
        "act": "pbi-full-deals",
        "pbi": CMAP_PBI_TOKEN,
        "page": 1
    }, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "ok":
        raise ValueError(f"API error: {body}")

    content = body["content"]
    total_pages = content["total_pages"]
    total = content["total"]
    print(f"      → Всего записей: {total}, страниц: {total_pages}")

    all_records = list(content["data"])

    if total_pages > 1:
        print(f"   🚀 Загрузка страниц 2-{total_pages} ({MAX_WORKERS} потоков)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_deals_page, p): p for p in range(2, total_pages + 1)}
            done_count = 0
            for future in as_completed(futures):
                page = futures[future]
                try:
                    all_records.extend(future.result())
                    done_count += 1
                    if done_count % 20 == 0:
                        print(f"      ... {done_count}/{total_pages - 1} страниц")
                except Exception as e:
                    print(f"   ⚠️ Ошибка на стр. {page}: {e}")

    df = pd.DataFrame(all_records)
    
    # Фильтр от 2020
    before = len(df)
    df = df[pd.to_numeric(df['agreement_date_y'], errors='coerce') >= YEAR_FROM]
    after = len(df)
    print(f"   🔍 Фильтр от {YEAR_FROM}: {before} → {after} (отброшено {before - after})")
    
    return df


def load_deals():
    """Загрузка сделок: API → raw.raw_cmap_deals"""
    print("\n📊 ЭТАП 1: СДЕЛКИ (pbi-full-deals)")
    df = fetch_deals()
    if df.empty:
        print("   ⚠️ Сделки: пустой результат")
        return 0
    df['_loaded_at'] = datetime.now().isoformat()
    load_to_postgres(df, "raw", "raw_cmap_deals")
    return len(df)

# ============================================================
# 2. ПРАЙСЫ (pbi-prices) — без пагинации
# ============================================================

def fetch_prices() -> pd.DataFrame:
    """Загружает прайсы из CMAP API"""
    print(f"   🌐 Прайсы: запрос данных...")

    resp = requests.get(CMAP_API_URL, params={
        "act": "pbi-prices",
        "pbi": CMAP_PBI_TOKEN
    }, timeout=PRICES_TIMEOUT, stream=True)
    resp.raise_for_status()

    body = resp.json()

    if body.get("status") != "ok":
        raise ValueError(f"API error: {body.get('content', 'unknown')}")

    data = body.get("content", [])

    # Если content — dict с пагинацией
    if isinstance(data, dict) and "data" in data:
        total = int(data.get("total", 0))
        total_pages = int(data.get("total_pages", 1))
        records = list(data["data"])
        print(f"      → Пагинация: {total} записей, {total_pages} страниц")

        for page in range(2, total_pages + 1):
            if page % 10 == 0:
                print(f"   🌐 Прайсы: страница {page}/{total_pages}...")
            resp_p = requests.get(CMAP_API_URL, params={
                "act": "pbi-prices",
                "pbi": CMAP_PBI_TOKEN,
                "page": page
            }, timeout=PRICES_TIMEOUT)
            resp_p.raise_for_status()
            body_p = resp_p.json()
            if body_p.get("status") == "ok":
                content_p = body_p["content"]
                if isinstance(content_p, dict) and "data" in content_p:
                    records.extend(content_p["data"])
                elif isinstance(content_p, list):
                    records.extend(content_p)

        data = records

    # Если content — list (без пагинации)
    elif isinstance(data, list):
        print(f"      → Без пагинации, получено {len(data)} записей")

    else:
        raise ValueError(f"Неожиданный формат content: {type(data)}")

    if not data:
        raise ValueError("API вернул пустой массив данных")

    df = pd.DataFrame(data)
    df = df.astype(str).replace({'None': None, 'nan': None, 'null': None})
    print(f"   ✅ Загружено {len(df)} записей из API")
    return df


def load_prices():
    """Загрузка прайсов: API → raw.raw_cmap_prices"""
    print("\n💰 ЭТАП 2: ПРАЙСЫ (pbi-prices)")
    df = fetch_prices()
    if df.empty:
        print("   ⚠️ Прайсы: пустой результат")
        return 0
    df['_loaded_at'] = datetime.now().isoformat()
    load_to_postgres(df, "raw", "raw_cmap_prices")
    return len(df)

# ============================================================
# ОСНОВНОЙ ЗАПУСК
# ============================================================

def run():
    start = datetime.now()
    print("=" * 60)
    print(f"🚀 CMAP ETL (Сделки + Прайсы) — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    errors = []
    total_rows = 0

    # Сделки
    try:
        total_rows += load_deals()
    except Exception as e:
        error_msg = f"Сделки: {str(e)[:200]}"
        print(f"   ❌ {error_msg}")
        errors.append(error_msg)

    # Прайсы
    try:
        total_rows += load_prices()
    except Exception as e:
        error_msg = f"Прайсы: {str(e)[:200]}"
        print(f"   ❌ {error_msg}")
        errors.append(error_msg)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*60}")

    if errors:
        print(f"⚠️ Завершено с ошибками за {elapsed:.1f} сек | {total_rows} строк")
        error_list = "\n".join([f"• {e}" for e in errors])
        send_telegram_alert(
            f"🚨 <b>CMAP ETL ОШИБКА</b>\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱️ {elapsed:.1f} сек\n\n{error_list}"
        )
    else:
        print(f"✅ Готово за {elapsed:.1f} сек | {total_rows} строк")


if __name__ == '__main__':
    run()
