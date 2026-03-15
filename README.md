# Аналитика ОП

Аналитическая платформа для отдела продаж недвижимости. Проект включает ETL-пайплайн, хранилище данных на PostgreSQL и интерактивные HTML-дашборды с REST API.

## Живые дашборды

- **Основной дашборд:** http://31.130.149.43:8000
- **Таблица лидеров:** http://31.130.149.43:8000/static/leaderboard.html

## Стек технологий

- **Python** — ETL, генерация данных, API
- **PostgreSQL** — хранилище данных (raw → staging → mart)
- **FastAPI** — REST API для дашбордов
- **SQL** — трансформации данных (17 скриптов)
- **HTML/JS** — интерактивные дашборды
- **Docker Compose** — деплой на сервер

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
├── docker-compose.yml          # Деплой: db + api + etl
├── БД/
│   ├── Dockerfile
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
    ├── Dockerfile
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
- RF-матрица сегментации
- Когортный анализ retention

**Таблица лидеров** (`leaderboard.html`) — рейтинг агентов и агентств по объёму продаж.

## Деплой

Проект задеплоен через Docker Compose (три контейнера: db, api, etl):

```bash
git clone https://github.com/maxim12rg-eng/realty-crm-dashboard.git
cd realty-crm-dashboard
cp .env.example .env  # заполнить переменные
docker-compose up -d
```
