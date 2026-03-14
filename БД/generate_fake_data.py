# generate_fake_data.py
# Генератор синтетических данных для всех RAW-таблиц
# Пишет напрямую в PostgreSQL схему raw (минуя MySQL)
# Запускать один раз перед etl_full_v6.py

import random
import os
from datetime import datetime, timedelta, date
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

load_dotenv(Path(__file__).parent / '.env')

fake = Faker('ru_RU')
random.seed(42)

# ============================================================
# ПОДКЛЮЧЕНИЕ
# ============================================================

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', 5432)),
        database=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
    )

def insert_df(conn, schema, table, df):
    """Вставка DataFrame через execute_values"""
    if df.empty:
        return
    cols = ', '.join(f'"{c}"' for c in df.columns)
    vals = [tuple(row) for row in df.itertuples(index=False)]
    sql = f'INSERT INTO {schema}."{table}" ({cols}) VALUES %s ON CONFLICT DO NOTHING'
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()
    print(f'  ✅ {schema}.{table}: {len(df)} строк')

def drop_create(conn, schema, table, ddl):
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table}" CASCADE')
        cur.execute(ddl)
    conn.commit()

# ============================================================
# СПРАВОЧНИКИ (фиксированные значения)
# ============================================================

OBJECTS = ['Парк Апрель', 'Берег', 'Новая Москва Парк', 'Солнечный', 'Грин Вилл']
AGENCIES = [
    (1001, 'Этажи'), (1002, 'Инком'), (1003, 'НДВ'), (1004, 'Бест Недвижимость'),
    (1005, 'Миэль'), (1006, 'Авито Недвижимость'), (1007, 'ЦИАН'),
    (1008, 'Перспектива24'), (1009, 'Сделка.ру'), (1010, 'Realty Group'),
]
ZIN_REASONS = ['Высокая цена', 'Не подошла планировка', 'Выбрал другой объект',
               'Не готов к покупке', 'Ипотека не одобрена', None]
SOURCES = ['SITE', 'CALL', 'SOCIAL', 'PARTNER', 'REPEAT', None]
CATEGORY_IDS = [5, 10]   # 5=Агентский, 10=Прямой
STAGE_IDS_BY_CAT = {
    5:  ['DEAL_STAGE_5:NEW', 'DEAL_STAGE_5:PREPARATION', 'DEAL_STAGE_5:WON', 'DEAL_STAGE_5:LOSE'],
    10: ['DEAL_STAGE_10:NEW', 'DEAL_STAGE_10:PREPARATION', 'DEAL_STAGE_10:WON', 'DEAL_STAGE_10:LOSE'],
    4:  ['DEAL_STAGE_4:NEW', 'DEAL_STAGE_4:WON'],
}

START_DATE = date(2025, 4, 1)
END_DATE   = date(2026, 3, 1)

def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_dt(start=START_DATE, end=END_DATE):
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(8, 20), random.randint(0, 59))

# ============================================================
# ГЕНЕРАТОРЫ ТАБЛИЦ
# ============================================================

def gen_b_user(n=30):
    """Пользователи Bitrix (МОПы + прочие)"""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            'id': str(i),
            'name': fake.first_name(),
            'last_name': fake.last_name(),
            'second_name': fake.middle_name(),
            'login': f'user{i}',
            'email': fake.email(),
            'active': 'Y',
        })
    return pd.DataFrame(rows)

def gen_b_iblock_element(mop_ids):
    """МОПы — элементы инфоблока 85"""
    rows = []
    mop_names = [
        'Иванов Иван', 'Петрова Мария', 'Сидоров Алексей', 'Козлова Наталья',
        'Новиков Дмитрий', 'Морозова Елена', 'Волков Сергей', 'Соколова Ирина',
        'Лебедев Андрей', 'Попова Анна',
    ]
    for i, mop_id in enumerate(mop_ids):
        rows.append({
            'id': str(mop_id),
            'iblock_id': '85',
            'name': mop_names[i % len(mop_names)],
            'code': f'mop_{mop_id}',
            'xml_id': f'mop_{mop_id}',
            'active': 'Y',
            'sort': str((i + 1) * 100),
            'date_create': rand_dt(date(2024, 1, 1), date(2024, 12, 31)).strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp_x': rand_dt(date(2025, 1, 1), date(2025, 6, 1)).strftime('%Y-%m-%d %H:%M:%S'),
            'active_from': None,
            'active_to': None,
            'created_by': '1',
            'modified_by': '1',
        })
    return pd.DataFrame(rows)

def gen_b_crm_company():
    """Агентства недвижимости"""
    rows = []
    for agency_id, agency_name in AGENCIES:
        rows.append({
            'id': str(agency_id),
            'title': agency_name,
            'company_type': 'PARTNER',
            'industry': 'REALTY',
            'category_id': '0',
            'created_by_id': '1',
            'modify_by_id': '1',
            'assigned_by_id': '1',
            'date_create': rand_dt(date(2023, 1, 1), date(2024, 1, 1)).strftime('%Y-%m-%d %H:%M:%S'),
            'date_modify': rand_dt(date(2024, 1, 1), date(2025, 1, 1)).strftime('%Y-%m-%d %H:%M:%S'),
            'last_activity_time': rand_dt().strftime('%Y-%m-%d %H:%M:%S'),
            'address': fake.address(),
            'address_legal': None,
            'banking_details': None,
            'has_phone': 'Y',
            'has_email': random.choice(['Y', 'N']),
            'is_my_company': 'N',
            'revenue': str(round(random.uniform(1e6, 1e8), 2)),
            'currency_id': 'RUB',
            'employees': random.choice(['EMPLOYEES_1', 'EMPLOYEES_2', 'EMPLOYEES_3']),
            'comments': None,
            'lead_id': None,
            'last_activity_by': '1',
        })
    return pd.DataFrame(rows)

def gen_b_crm_contact(agent_ids, agency_map):
    """Агенты — контакты Bitrix"""
    rows = []
    for agent_id, agency_id in zip(agent_ids, agency_map):
        rows.append({
            'id': str(agent_id),
            'full_name': fake.name(),
            'name': fake.first_name(),
            'last_name': fake.last_name(),
            'second_name': fake.middle_name(),
            'company_id': str(agency_id),
            'type_id': 'AGENT',
            'created_by_id': '1',
            'modify_by_id': '1',
            'assigned_by_id': '1',
            'date_create': rand_dt(date(2023, 1, 1), date(2024, 6, 1)).strftime('%Y-%m-%d %H:%M:%S'),
            'date_modify': rand_dt(date(2024, 6, 1), date(2025, 6, 1)).strftime('%Y-%m-%d %H:%M:%S'),
            'last_activity_time': rand_dt().strftime('%Y-%m-%d %H:%M:%S'),
            'source_id': random.choice(SOURCES),
            'source_description': None,
            'post': 'Агент',
            'address': None,
            'comments': None,
            'has_phone': 'Y',
            'has_email': random.choice(['Y', 'N']),
            'category_id': '0',
        })
    return pd.DataFrame(rows)

def gen_b_crm_field_multi(agent_ids):
    """Телефоны агентов"""
    rows = []
    for agent_id in agent_ids:
        rows.append({
            'element_id': str(agent_id),
            'entity_id': 'CONTACT',
            'type_id': 'PHONE',
            'value': 'test_phone',
        })
    return pd.DataFrame(rows)

def gen_b_crm_status():
    """Справочник статусов/стадий сделок"""
    rows = []
    stages = {
        5: [
            ('DEAL_STAGE_5:NEW', 'Новая', 10, None),
            ('DEAL_STAGE_5:PREPARATION', 'Переговоры', 20, None),
            ('DEAL_STAGE_5:WON', 'Сделка', 30, 'S'),
            ('DEAL_STAGE_5:LOSE', 'Отказ', 40, 'F'),
        ],
        10: [
            ('DEAL_STAGE_10:NEW', 'Новая', 10, None),
            ('DEAL_STAGE_10:PREPARATION', 'Переговоры', 20, None),
            ('DEAL_STAGE_10:WON', 'Сделка', 30, 'S'),
            ('DEAL_STAGE_10:LOSE', 'Отказ', 40, 'F'),
        ],
        4: [
            ('DEAL_STAGE_4:NEW', 'В работе', 10, None),
            ('DEAL_STAGE_4:WON', 'Завершено', 20, 'S'),
        ],
    }
    sid = 1
    for cat_id, stage_list in stages.items():
        entity_id = f'DEAL_STAGE_{cat_id}'
        for status_id, name, sort, semantics in stage_list:
            rows.append({
                'id': str(sid),
                'entity_id': entity_id,
                'status_id': status_id,
                'name': name,
                'name_init': name,
                'sort': str(sort),
                'system': 'N',
                'color': '#9DCF00',
                'semantics': semantics,
                'category_id': str(cat_id),
            })
            sid += 1
    return pd.DataFrame(rows)

def gen_b_user_field_enum():
    """Справочник причин ЗИН"""
    rows = []
    # user_field_id=166 для category_id=5 (АОП)
    for i, reason in enumerate(ZIN_REASONS):
        if reason:
            rows.append({
                'id': str(100 + i),
                'user_field_id': '166',
                'value': reason,
                'sort': str((i + 1) * 10),
            })
    # user_field_id=3298 для category_id=10 (Прямые)
    for i, reason in enumerate(ZIN_REASONS):
        if reason:
            rows.append({
                'id': str(200 + i),
                'user_field_id': '3298',
                'value': reason,
                'sort': str((i + 1) * 10),
            })
    return pd.DataFrame(rows)

def gen_b_crm_deal_category():
    rows = [
        {'id': '4', 'name': 'Сопровождение', 'sort': '10', 'is_locked': 'N'},
        {'id': '5', 'name': 'АОП (Агентский)', 'sort': '20', 'is_locked': 'N'},
        {'id': '10', 'name': 'Прямые продажи', 'sort': '30', 'is_locked': 'N'},
    ]
    return pd.DataFrame(rows)

def gen_deals(n_deals, mop_ids, agent_ids, agency_ids):
    """
    Основные таблицы сделок: b_crm_deal + b_uts_crm_deal
    Воронка: запись → показ → бронь → сделка
    """
    deals = []
    uts   = []

    # Часть сделок — сопровождение (category_id=4), связаны с основными
    n_main    = int(n_deals * 0.85)
    n_support = n_deals - n_main

    deal_ids_main = list(range(1000, 1000 + n_main))

    for deal_id in deal_ids_main:
        category_id = random.choices(CATEGORY_IDS, weights=[70, 30])[0]
        mop_id      = random.choice(mop_ids)
        agent_id    = random.choice(agent_ids)
        agency_id   = random.choice(agency_ids)
        stage_id    = random.choice(STAGE_IDS_BY_CAT[category_id])
        date_create = rand_dt()
        date_modify = date_create + timedelta(days=random.randint(0, 60))
        is_test     = False

        # Воронка: у каждой сделки может быть несколько этапов
        # запись → (80%) показ → (60%) бронь → (30%) сделка
        appointed_show_dt = rand_date(date_create.date(), min(date_create.date() + timedelta(days=14), END_DATE)).strftime('%Y-%m-%d') if random.random() < 0.9 else None
        show_dt           = (date_create.date() + timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d') if appointed_show_dt and random.random() < 0.80 else None
        reserve_dt        = (date_create.date() + timedelta(days=random.randint(15, 30))).strftime('%Y-%m-%d') if show_dt and random.random() < 0.45 else None
        deal_dt           = (date_create.date() + timedelta(days=random.randint(31, 60))).strftime('%Y-%m-%d') if reserve_dt and random.random() < 0.65 else None

        zin_enum_id = None
        if not deal_dt and random.random() < 0.4:
            zin_enum_id = str(random.choice([100, 101, 102, 103, 104]))

        deal_amount       = round(random.uniform(5_000_000, 25_000_000), 2) if deal_dt else None
        reserve_lot_sum   = round(random.uniform(3_000_000, 20_000_000), 2) if reserve_dt else None
        area              = round(random.uniform(30, 120), 2)

        deals.append({
            'id': str(deal_id),
            'title': f'Сделка {deal_id}',
            'category_id': str(category_id),
            'stage_id': stage_id,
            'assigned_by_id': str(random.choice(mop_ids)),
            'company_id': str(agency_id) + '.0',
            'contact_id': str(agent_id) + '.0',
            'opportunity': str(deal_amount or 0),
            'date_create': date_create.strftime('%Y-%m-%d %H:%M:%S'),
            'date_modify': date_modify.strftime('%Y-%m-%d %H:%M:%S'),
            'source_id': random.choice(SOURCES),
        })

        uts.append({
            'value_id': str(deal_id),
            'uf_crm_responsible_mop': str(mop_id) + '.0',
            'uf_fact_presentation_date': show_dt,
            'uf_crm_booking_contract_date': reserve_dt,
            'uf_crm_contract_date': deal_dt,
            'uf_plan_showing_date': appointed_show_dt,
            'uf_crm_1690293096296': str(deal_amount) if deal_amount else None,
            'uf_booked_price': str(reserve_lot_sum) if reserve_lot_sum else None,
            'uf_area': str(area),
            'uf_crm_object': random.choice(OBJECTS),
            'uf_crm_agency': str(agency_id),
            'uf_crm_agent': str(agent_id),
            'uf_crm_sales_deal': None,
            'uf_refusal_reason': zin_enum_id,
            'uf_crm_1692888387831': None,
            'uf_last_4_phone_numbers': str(random.randint(1000, 9999)),
            'uf_crm_down_payment_date': None,
        })

    # Сделки сопровождения (category_id=4) — привязаны к основным
    support_start_id = 2000
    for i in range(n_support):
        linked_deal = random.choice(deal_ids_main[:n_main // 2])  # берём только те у кого могла быть сделка
        deal_id = support_start_id + i
        date_create = rand_dt()

        pv_paid_dt = (date_create.date() + timedelta(days=random.randint(45, 90))).strftime('%Y-%m-%d') if random.random() < 0.7 else None

        deals.append({
            'id': str(deal_id),
            'title': f'Сопровождение {deal_id}',
            'category_id': '4',
            'stage_id': random.choice(STAGE_IDS_BY_CAT[4]),
            'assigned_by_id': str(random.choice(mop_ids)),
            'company_id': None,
            'contact_id': None,
            'opportunity': '0',
            'date_create': date_create.strftime('%Y-%m-%d %H:%M:%S'),
            'date_modify': date_create.strftime('%Y-%m-%d %H:%M:%S'),
            'source_id': None,
        })

        uts.append({
            'value_id': str(deal_id),
            'uf_crm_responsible_mop': None,
            'uf_fact_presentation_date': None,
            'uf_crm_booking_contract_date': None,
            'uf_crm_contract_date': None,
            'uf_plan_showing_date': None,
            'uf_crm_1690293096296': None,
            'uf_booked_price': None,
            'uf_area': None,
            'uf_crm_object': None,
            'uf_crm_agency': None,
            'uf_crm_agent': None,
            'uf_crm_sales_deal': str(linked_deal) + '.0',
            'uf_refusal_reason': None,
            'uf_crm_1692888387831': None,
            'uf_last_4_phone_numbers': None,
            'uf_crm_down_payment_date': pv_paid_dt,
        })

    return pd.DataFrame(deals), pd.DataFrame(uts)

# ============================================================
# Excel-источники (raw_fg, raw_podpisannye, raw_v_rabote)
# ============================================================

def gen_raw_fg(n=300):
    """Контроль качества показов (Tager Excel)"""
    rows = []
    mop_names = ['Иванов Иван', 'Петрова Мария', 'Сидоров Алексей',
                 'Козлова Наталья', 'Новиков Дмитрий']
    for _ in range(n):
        score = lambda: str(round(random.uniform(0, 1), 2))
        d = rand_date()
        rows.append({
            'дата': d.strftime('%Y-%m-%d'),
            'длина': f'{random.randint(30, 120)} мин',
            'менеджер': random.choice(mop_names),
            'теплота': random.choice(['горячий', 'тёплый', 'холодный']),
            'энергичный_настрой': score(), 'приветствие': score(), 'имя': score(),
            'забота': score(), 'установление_контакта': score(), 'фрейминг': score(),
            'что_смотрели': score(), 'программирование': score(), 'цель_покупки': score(),
            'квалификация': score(), 'потребность': score(), 'боль': score(),
            'выход_на_лпр_работа_с_лпр': score(), 'резюмирование': score(),
            'презентация': score(), 'предзакрытие': score(), 'индивидуальный_оффер': score(),
            'кэв': score(), 'ускоритель_1_дефицит': score(), 'как_проходит_сделка_бронь': score(),
            'следующий_шаг': score(), 'соблюдение_структуры_встречи': score(),
            'инвестиционная_привлекательность': score(), 'ответы_на_вопросы': score(),
            'речь': score(), 'качество_записи': score(),
            'как_часто_планируете_приезжать': score(), 'срок_приобретения_к': score(),
            'бюджет_к': score(), 'способ_приобретения_наличные_ипот': score(),
            'лпр_к': score(), 'есть_ли_опыт_инвестирования_желае': score(),
            'цель_инвестирования_краткосрочны': score(),
            'неучтенное': score(), 'подсчет_баллов': str(round(random.uniform(0, 30), 2)),
            'fg': str(round(random.uniform(0.3, 1.0), 2)),
            'бронь': random.choice(['Да', 'Нет', None]),
            'дата_брони': rand_date().strftime('%Y-%m-%d') if random.random() < 0.3 else None,
            'unnamed:_42': None, 'оценка_возражений': score(), 'возражения': None,
            'unnamed:_45': None, 'неделя': str(d.isocalendar()[1]),
            'день': str(d.day), 'месяц': str(d.month), 'год': str(d.year),
            'комментарий': None,
            'сделка': f'/crm/deal/{random.randint(1000, 1500)}/' if random.random() < 0.5 else None,
            'оценка': random.choice(['хорошо', 'удовлетворительно', 'плохо']),
            'отдел_продаж': 'Агентский',
            'асессор': fake.name(),
            'source_file': 'fake_data.xlsx',
            'loaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
    return pd.DataFrame(rows)

def gen_raw_podpisannye(n=150):
    """Подписанные договоры (Nextcloud Excel)"""
    rows = []
    for i in range(n):
        agency = random.choice(AGENCIES)
        rows.append({
            'num': str(i + 1),
            'дата_брони': rand_date().strftime('%Y-%m-%d'),
            'объект': random.choice(OBJECTS),
            'номер_лота': str(random.randint(100, 999)),
            'здание': str(random.randint(1, 5)),
            's_факт': str(round(random.uniform(30, 120), 2)),
            's_по_выписке_проектe': str(round(random.uniform(30, 120), 2)),
            's_террасы': str(round(random.uniform(0, 20), 2)),
            'сумма_сделки': str(round(random.uniform(5_000_000, 25_000_000), 2)),
            'цена_за_кв_м': str(round(random.uniform(150_000, 400_000), 2)),
            'клиент': fake.name(),
            'размер_пв': str(round(random.uniform(0.1, 0.5), 4)),
            'срок_рассрочки_мес': str(random.choice([0, 6, 12, 24, 36])),
            'порядок_рассрочки': random.choice(['Ежемесячно', 'Ежеквартально', None]),
            'спец_условия': None,
            'мос_брокер': random.choice(['Да', 'Нет']),
            'менеджер': fake.name(),
            'агент': fake.name(),
            'агентство': agency[1],
            'размер_комисси': str(round(random.uniform(0.02, 0.05), 4)),
            'оплачено_комиссии': str(round(random.uniform(50_000, 500_000), 2)),
            'дата_в_договоре': rand_date().strftime('%Y-%m-%d'),
            'дата_регистрации': rand_date().strftime('%Y-%m-%d'),
            'дата_пв': rand_date().strftime('%Y-%m-%d') if random.random() < 0.6 else None,
            'количество_рабочих_дней_в_работе': str(random.randint(1, 90)),
            'ссылка': None,
            'комментарий': None,
        })
    return pd.DataFrame(rows)

def gen_raw_v_rabote(n=80):
    """В работе (Nextcloud Excel) — те же колонки без нескольких финансовых"""
    rows = []
    for i in range(n):
        agency = random.choice(AGENCIES)
        rows.append({
            'num': str(i + 1),
            'дата_брони': rand_date().strftime('%Y-%m-%d'),
            'объект': random.choice(OBJECTS),
            'номер_лота': str(random.randint(100, 999)),
            'здание': str(random.randint(1, 5)),
            's_факт': str(round(random.uniform(30, 120), 2)),
            's_по_выписке_проектe': str(round(random.uniform(30, 120), 2)),
            's_террасы': str(round(random.uniform(0, 20), 2)),
            'сумма_сделки': str(round(random.uniform(5_000_000, 25_000_000), 2)),
            'цена_за_кв_м': str(round(random.uniform(150_000, 400_000), 2)),
            'клиент': fake.name(),
            'размер_пв': str(round(random.uniform(0.1, 0.5), 4)),
            'срок_рассрочки_мес': str(random.choice([0, 6, 12, 24, 36])),
            'порядок_рассрочки': random.choice(['Ежемесячно', 'Ежеквартально', None]),
            'спец_условия': None,
            'мос_брокер': random.choice(['Да', 'Нет']),
            'менеджер': fake.name(),
            'агент': fake.name(),
            'агентство': agency[1],
            'размер_комисси': str(round(random.uniform(0.02, 0.05), 4)),
        })
    return pd.DataFrame(rows)

# ============================================================
# API bnMAP (raw_cmap_deals, raw_cmap_prices)
# ============================================================

DEVELOPERS = ['ПИК', 'Самолёт', 'А101', 'Эталон', 'ФСК']
CITIES = ['Москва', 'Московская область']
DISTRICTS = ['Южный', 'Северный', 'Западный', 'Восточный', 'Центральный']
LOT_TYPES = ['квартира', 'апартаменты', 'нежилое помещение']
BANKS = ['Сбербанк', 'ВТБ', 'Альфа-Банк', 'Газпромбанк', None]

def gen_raw_cmap_deals(n=500):
    rows = []
    for i in range(n):
        sq = round(random.uniform(25, 130), 2)
        price_sqm = round(random.uniform(150_000, 450_000), 2)
        rows.append({
            'hc_id_hash': f'obj_{random.randint(1, 20):04d}',
            'hc_name': random.choice(OBJECTS),
            'city': random.choice(CITIES),
            'loc_district': random.choice(DISTRICTS),
            'class': random.choice(['эконом', 'комфорт', 'бизнес', 'премиум']),
            'developer': random.choice(DEVELOPERS),
            'agreement_date': rand_date().strftime('%Y-%m-%d'),
            'ot_name': random.choice(LOT_TYPES),
            'floor': str(random.randint(1, 25)),
            'do_square': str(sq),
            'est_budget': str(round(sq * price_sqm, 2)),
            'price_square_r': str(price_sqm),
            'interior': random.choice(['чистовая', 'предчистовая', 'без отделки']),
            'bt_name': random.choice(['однокомнатная', 'двухкомнатная', 'трёхкомнатная', 'студия']),
            'mortgage': random.choice(['Да', 'Нет']),
            'bank_name': random.choice(BANKS),
            'mortgage_term': str(random.choice([0, 120, 180, 240, 300])) if random.random() < 0.5 else None,
        })
    return pd.DataFrame(rows)

def gen_raw_cmap_prices(n=300):
    rows = []
    for i in range(n):
        sq = round(random.uniform(25, 130), 3)
        price_sqm = round(random.uniform(150_000, 450_000), 2)
        price = round(sq * price_sqm, 2)
        rows.append({
            'id': str(10000 + i),
            'hc_name': random.choice(OBJECTS),
            'hc_id_hash': f'obj_{random.randint(1, 20):04d}',
            'developer': random.choice(DEVELOPERS),
            'builder': random.choice(DEVELOPERS),
            'region': 'Москва и МО',
            'city': random.choice(CITIES),
            'district': random.choice(DISTRICTS),
            'building': f'Корпус {random.randint(1, 5)}',
            'b_id_hash': f'bld_{random.randint(1, 50):04d}',
            'section': str(random.randint(1, 4)),
            'floor': str(random.randint(1, 25)),
            'numapartment': str(random.randint(1, 200)),
            'rooms': random.choice(['1', '2', '3', 'С']),
            'square': str(sq),
            'squareprice': str(price_sqm),
            'price': str(price),
            'interior': random.choice(['чистовая', 'предчистовая', 'без отделки']),
            'object_type': random.choice(['квартира', 'апартаменты']),
            'object_class': random.choice(['эконом', 'комфорт', 'бизнес']),
            'discount': random.choice(['Y', 'N']),
            'discount_value': str(round(random.uniform(0, 500_000), 2)) if random.random() < 0.2 else None,
            'agreement': random.choice(['ДДУ', 'ПДКП', 'ДКП']),
            'dsc': rand_date(date(2025, 6, 1), date(2026, 12, 1)).strftime('%Y-%m-%d'),
            'start_sales': rand_date(date(2023, 1, 1), date(2025, 1, 1)).strftime('%Y-%m-%d'),
            'stage': random.choice(['котлован', 'монтаж', 'отделка', 'сдан']),
            'construction': random.choice(['монолит', 'панель', 'кирпич']),
            'first_lot_date': rand_date(date(2023, 1, 1), date(2024, 1, 1)).strftime('%Y-%m-%d'),
            'first_lot_price_square': str(round(price_sqm * 0.85, 2)),
            'first_lot_price': str(round(price * 0.85, 2)),
            'lot_term': str(random.randint(7, 30)),
            'lot_price_square_rise': str(round(random.uniform(0.001, 0.05), 6)),
            'lot_price_rise': str(round(random.uniform(0.001, 0.05), 6)),
            'first_appearance': rand_date(date(2023, 1, 1), date(2024, 1, 1)).strftime('%Y-%m-%d'),
            'create_time': rand_dt().strftime('%Y-%m-%d %H:%M:%S'),
            'a_lot_hash': f'lot_{10000 + i:06d}',
            'pbo_id_hash': f'pbo_{random.randint(1, 100):04d}',
        })
    return pd.DataFrame(rows)

# ============================================================
# DDL для RAW таблиц (все колонки TEXT, как в реальном Bitrix)
# ============================================================

DDL = {
    'b_user': """CREATE TABLE raw.b_user (
        id TEXT, name TEXT, last_name TEXT, second_name TEXT,
        login TEXT, email TEXT, active TEXT)""",

    'b_iblock_element': """CREATE TABLE raw.b_iblock_element (
        id TEXT, iblock_id TEXT, name TEXT, code TEXT, xml_id TEXT,
        active TEXT, sort TEXT, date_create TEXT, timestamp_x TEXT,
        active_from TEXT, active_to TEXT, created_by TEXT, modified_by TEXT)""",

    'b_crm_company': """CREATE TABLE raw.b_crm_company (
        id TEXT, title TEXT, company_type TEXT, industry TEXT, category_id TEXT,
        created_by_id TEXT, modify_by_id TEXT, assigned_by_id TEXT,
        date_create TEXT, date_modify TEXT, last_activity_time TEXT,
        address TEXT, address_legal TEXT, banking_details TEXT,
        has_phone TEXT, has_email TEXT, is_my_company TEXT,
        revenue TEXT, currency_id TEXT, employees TEXT, comments TEXT,
        lead_id TEXT, last_activity_by TEXT)""",

    'b_crm_contact': """CREATE TABLE raw.b_crm_contact (
        id TEXT, full_name TEXT, name TEXT, last_name TEXT, second_name TEXT,
        company_id TEXT, type_id TEXT, created_by_id TEXT, modify_by_id TEXT,
        assigned_by_id TEXT, date_create TEXT, date_modify TEXT,
        last_activity_time TEXT, source_id TEXT, source_description TEXT,
        post TEXT, address TEXT, comments TEXT, has_phone TEXT,
        has_email TEXT, category_id TEXT)""",

    'b_crm_deal': """CREATE TABLE raw.b_crm_deal (
        id TEXT, title TEXT, category_id TEXT, stage_id TEXT,
        assigned_by_id TEXT, company_id TEXT, contact_id TEXT,
        opportunity TEXT, date_create TEXT, date_modify TEXT, source_id TEXT)""",

    'b_uts_crm_deal': """CREATE TABLE raw.b_uts_crm_deal (
        value_id TEXT, uf_crm_responsible_mop TEXT,
        uf_fact_presentation_date TEXT, uf_crm_booking_contract_date TEXT,
        uf_crm_contract_date TEXT, uf_plan_showing_date TEXT,
        uf_crm_1690293096296 TEXT, uf_booked_price TEXT, uf_area TEXT,
        uf_crm_object TEXT, uf_crm_agency TEXT, uf_crm_agent TEXT,
        uf_crm_sales_deal TEXT, uf_refusal_reason TEXT,
        uf_crm_1692888387831 TEXT, uf_last_4_phone_numbers TEXT,
        uf_crm_down_payment_date  TEXT)""",

    'b_crm_status': """CREATE TABLE raw.b_crm_status (
        id TEXT, entity_id TEXT, status_id TEXT, name TEXT, name_init TEXT,
        sort TEXT, system TEXT, color TEXT, semantics TEXT, category_id TEXT)""",

    'b_user_field_enum': """CREATE TABLE raw.b_user_field_enum (
        id TEXT, user_field_id TEXT, value TEXT, sort TEXT)""",

    'b_crm_deal_category': """CREATE TABLE raw.b_crm_deal_category (
        id TEXT, name TEXT, sort TEXT, is_locked TEXT)""",
    
    'b_crm_field_multi': """CREATE TABLE raw.b_crm_field_multi (
        element_id TEXT, entity_id TEXT, type_id TEXT, value TEXT)""",

    'raw_fg': """CREATE TABLE raw.raw_fg (
        дата TEXT, длина TEXT, менеджер TEXT, теплота TEXT,
        энергичный_настрой TEXT, приветствие TEXT, имя TEXT, забота TEXT,
        установление_контакта TEXT, фрейминг TEXT, что_смотрели TEXT,
        программирование TEXT, цель_покупки TEXT, квалификация TEXT,
        потребность TEXT, боль TEXT, выход_на_лпр_работа_с_лпр TEXT,
        резюмирование TEXT, презентация TEXT, предзакрытие TEXT,
        индивидуальный_оффер TEXT, кэв TEXT, ускоритель_1_дефицит TEXT,
        как_проходит_сделка_бронь TEXT, следующий_шаг TEXT,
        соблюдение_структуры_встречи TEXT, инвестиционная_привлекательность TEXT,
        ответы_на_вопросы TEXT, речь TEXT, качество_записи TEXT,
        как_часто_планируете_приезжать TEXT, срок_приобретения_к TEXT,
        бюджет_к TEXT, способ_приобретения_наличные_ипот TEXT, лпр_к TEXT,
        есть_ли_опыт_инвестирования_желае TEXT, цель_инвестирования_краткосрочны TEXT,
        неучтенное TEXT, подсчет_баллов TEXT, fg TEXT, бронь TEXT,
        дата_брони TEXT, "unnamed:_42" TEXT, оценка_возражений TEXT,
        возражения TEXT, "unnamed:_45" TEXT, неделя TEXT, день TEXT,
        месяц TEXT, год TEXT, комментарий TEXT, сделка TEXT,
        оценка TEXT, отдел_продаж TEXT, асессор TEXT,
        source_file TEXT, loaded_at TEXT)""",

    'raw_podpisannye': """CREATE TABLE raw.raw_podpisannye (
        num TEXT, дата_брони TEXT, объект TEXT, номер_лота TEXT, здание TEXT,
        s_факт TEXT, "s_по_выписке_проектe" TEXT, s_террасы TEXT,
        сумма_сделки TEXT, цена_за_кв_м TEXT, клиент TEXT, размер_пв TEXT,
        срок_рассрочки_мес TEXT, порядок_рассрочки TEXT, спец_условия TEXT,
        мос_брокер TEXT, менеджер TEXT, агент TEXT, агентство TEXT,
        размер_комисси TEXT, оплачено_комиссии TEXT, дата_в_договоре TEXT,
        дата_регистрации TEXT, дата_пв TEXT,
        количество_рабочих_дней_в_работе TEXT, ссылка TEXT, комментарий TEXT)""",

    'raw_v_rabote': """CREATE TABLE raw.raw_v_rabote (
        num TEXT, дата_брони TEXT, объект TEXT, номер_лота TEXT, здание TEXT,
        s_факт TEXT, "s_по_выписке_проектe" TEXT, s_террасы TEXT,
        сумма_сделки TEXT, цена_за_кв_м TEXT, клиент TEXT, размер_пв TEXT,
        срок_рассрочки_мес TEXT, порядок_рассрочки TEXT, спец_условия TEXT,
        мос_брокер TEXT, менеджер TEXT, агент TEXT, агентство TEXT,
        размер_комисси TEXT)""",

    'raw_cmap_deals': """CREATE TABLE raw.raw_cmap_deals (
        hc_id_hash TEXT, hc_name TEXT, city TEXT, loc_district TEXT,
        class TEXT, developer TEXT, agreement_date TEXT, ot_name TEXT,
        floor TEXT, do_square TEXT, est_budget TEXT, price_square_r TEXT,
        interior TEXT, bt_name TEXT, mortgage TEXT, bank_name TEXT,
        mortgage_term TEXT)""",

    'raw_cmap_prices': """CREATE TABLE raw.raw_cmap_prices (
        id TEXT, hc_name TEXT, hc_id_hash TEXT, developer TEXT, builder TEXT,
        region TEXT, city TEXT, district TEXT, building TEXT, b_id_hash TEXT,
        section TEXT, floor TEXT, numapartment TEXT, rooms TEXT, square TEXT,
        squareprice TEXT, price TEXT, interior TEXT, object_type TEXT,
        object_class TEXT, discount TEXT, discount_value TEXT, agreement TEXT,
        dsc TEXT, start_sales TEXT, stage TEXT, construction TEXT,
        first_lot_date TEXT, first_lot_price_square TEXT, first_lot_price TEXT,
        lot_term TEXT, lot_price_square_rise TEXT, lot_price_rise TEXT,
        first_appearance TEXT, create_time TEXT, a_lot_hash TEXT, pbo_id_hash TEXT)""",
}

# ============================================================
# MAIN
# ============================================================

def main():
    print('🚀 Генерация синтетических данных...\n')

    conn = get_conn()

    # Создаём схему raw если нет
    with conn.cursor() as cur:
        for schema in ['raw', 'staging', 'mart', 'bot']:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    conn.commit()

    # Фиксированные ID
    mop_ids    = list(range(2544100, 2544110))   # 10 МОПов
    agency_ids = [a[0] for a in AGENCIES]         # 10 агентств
    agent_ids  = list(range(5000, 5060))          # 60 агентов
    agency_map = [random.choice(agency_ids) for _ in agent_ids]

    # Создаём и заполняем таблицы
    tables = [
        ('b_user',            gen_b_user()),
        ('b_iblock_element',  gen_b_iblock_element(mop_ids)),
        ('b_crm_company',     gen_b_crm_company()),
        ('b_crm_contact',     gen_b_crm_contact(agent_ids, agency_map)),
        ('b_crm_field_multi', gen_b_crm_field_multi(agent_ids)),
        ('b_crm_status',      gen_b_crm_status()),
        ('b_user_field_enum', gen_b_user_field_enum()),
        ('b_crm_deal_category', gen_b_crm_deal_category()),
        ('raw_fg',            gen_raw_fg(300)),
        ('raw_podpisannye',   gen_raw_podpisannye(150)),
        ('raw_v_rabote',      gen_raw_v_rabote(80)),
        ('raw_cmap_deals',    gen_raw_cmap_deals(500)),
        ('raw_cmap_prices',   gen_raw_cmap_prices(300)),
    ]

    # Сделки генерируем отдельно (две таблицы)
    df_deals, df_uts = gen_deals(600, mop_ids, agent_ids, agency_ids)

    for table_name, df in tables:
        drop_create(conn, 'raw', table_name, DDL[table_name])
        insert_df(conn, 'raw', table_name, df)

    drop_create(conn, 'raw', 'b_crm_deal', DDL['b_crm_deal'])
    insert_df(conn, 'raw', 'b_crm_deal', df_deals)

    drop_create(conn, 'raw', 'b_uts_crm_deal', DDL['b_uts_crm_deal'])
    insert_df(conn, 'raw', 'b_uts_crm_deal', df_uts)

    conn.close()
    print('\n✅ Все таблицы сгенерированы. Теперь запускай SQL-скрипты staging → mart.')

if __name__ == '__main__':
    main()
