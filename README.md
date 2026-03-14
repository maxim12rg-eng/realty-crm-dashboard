# Аналитика ОП

Аналитическая платформа для отдела продаж недвижимости. Проект включает ETL-пайплайн, хранилище данных на PostgreSQL и интерактивные HTML-дашборды с REST API.

## Стек технологий

- **Python** — ETL, генерация данных, API
- **PostgreSQL** — хранилище данных (raw → staging → mart)
- **FastAPI** — REST API для дашбордов
- **SQL** — трансформации данных (17 скриптов)
- **HTML/JS** — интерактивные дашборды

## Архитектура

```
Источники данных
├── MySQL (Bitrix24 CRM)       — сделки, контакты, компании
├── Excel (Tager)              — контроль качества показов
├── Excel (Nextcloud)          — отдел сопровождения
└── REST API (bnMAP.pro)       — данные рынка недвижимости
         │
         ▼
    raw schema (PostgreSQL)
         │
         ▼
    staging schema             — очистка и нормализация
         │
         ▼
    mart schema                — витрины данных для дашбордов
         │
         ▼
    FastAPI → HTML Дашборды
```

## Структура проекта

```
├── БД/
│   ├── etl_full_v6.py          # Основной ETL: RAW → staging → mart
│   ├── generate_fake_data.py   # Генератор синтетических данных
│   ├── load_all_tables.py      # Загрузка таблиц из MySQL (Bitrix24)
│   ├── load_tager_fg.py        # Загрузка Excel файлов (Tager)
│   ├── load_soprovozhdenie.py  # Загрузка Excel файлов (Nextcloud)
│   ├── load_cmap_deals.py      # Загрузка данных из API bnMAP.pro
│   ├── dwh_requirements.txt    # Зависимости
│   └── sql/                    # SQL-скрипты трансформаций
│       ├── 01_staging_mops.sql
│       ├── 02_staging_company.sql
│       ├── ...
│       └── 17_rang_agent.sql
└── Дашборд/
    ├── api_server.py           # FastAPI сервер
    └── static/
        ├── index.html          # Основной дашборд
        └── leaderboard.html    # Таблица лидеров
```

## ETL Pipeline

Пайплайн запускается каждые 10 минут и включает:

1. **RAW слой** — инкрементальная загрузка из MySQL (Bitrix24) с поддержкой чанков и атомарной записью
2. **Excel источники** — загрузка файлов контроля качества и сопровождения
3. **API источник** — загрузка данных рынка недвижимости из bnMAP.pro
4. **Staging слой** — очистка, нормализация, приведение типов
5. **Mart слой** — воронка продаж, рейтинги агентов и агентств

## Дашборды

**Основной дашборд** (`index.html`) — аналитика по агентскому отделу:
- Воронка продаж (запись → показ → бронь → сделка → ПВ)
- Churn-анализ агентов и агентств
- Динамика по периодам

**Таблица лидеров** (`leaderboard.html`) — рейтинг агентов и агентств по объёму продаж.

## Быстрый старт

### 1. Установить зависимости

```bash
pip install -r БД/dwh_requirements.txt
pip install fastapi uvicorn "psycopg[binary]"
```

### 2. Настроить переменные окружения

Создать файл `БД/.env`:
```env
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=realty_crm
PG_USER=postgres
PG_PASSWORD=your_password
ETL_BOT_TOKEN=your_telegram_bot_token
```

Создать файл `Дашборд/.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realty_crm
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Создать базу данных PostgreSQL

```sql
CREATE DATABASE realty_crm;
```

### 4. Сгенерировать синтетические данные

```bash
cd БД
python generate_fake_data.py
```

### 5. Запустить ETL

```bash
python etl_full_v6.py
```

### 6. Запустить API

```bash
cd Дашборд
python -m uvicorn api_server:app --reload --port 8000
```

### 7. Открыть дашборд

```
http://localhost:8000
http://localhost:8000/static/leaderboard.html
```
