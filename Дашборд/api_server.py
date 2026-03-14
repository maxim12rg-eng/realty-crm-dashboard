import os, json, io
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env
load_dotenv(Path(__file__).parent / '.env')

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), 
}

START_DATE = "2025-04-01"

def get_conn():
    return psycopg.connect(**DB_CONFIG)

def qdb(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or {})
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

class Enc(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (date, datetime)): return o.isoformat()
        if isinstance(o, Decimal): return float(o)
        return super().default(o)

def jr(data):
    return JSONResponse(json.loads(json.dumps(data, cls=Enc)))

BASE = f"""
    f.is_test = FALSE
    AND f.sales_department = 'Агентский'
    AND COALESCE(f.agency_name, '') != 'Nedvex'
    AND f.stage_date > '1900-01-01'::date
"""
DATE_FILTER = f"AND f.stage_date >= '{START_DATE}'::date"

app = FastAPI(title="AgentHub v4")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(STATIC):
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

@app.get("/")
async def root():
    p = os.path.join(STATIC, "index.html")
    return FileResponse(p) if os.path.exists(p) else {"msg": "put index.html in static/"}


# ═══════════════════════════════════
# CHURN
# ═══════════════════════════════════

def churn_cte(entity):
    eid = "f.agent_id" if entity == "agent" else "f.agency_id"
    enm = "f.agent_name" if entity == "agent" else "f.agency_name"

    pjoin = ""
    pfield = "NULL::text AS agent_phone"
    if entity == "agent":
        pjoin = f"""LEFT JOIN (
            SELECT DISTINCT ON (agent_id) agent_id, agent_phone
            FROM mart.deals WHERE agent_phone IS NOT NULL AND agent_phone!=''
            ORDER BY agent_id, deal_dt DESC
        ) ph ON {eid}=ph.agent_id"""
        pfield = "ph.agent_phone"

    acnt = f"""COUNT(DISTINCT CASE WHEN f.stage_name='Показ' AND f.stage_date >= CURRENT_DATE - INTERVAL '90 days' THEN f.agent_id END) AS active_agents_count,
        COUNT(DISTINCT f.agent_id) AS total_agents_count,""" if entity == "agency" else "NULL::int AS active_agents_count, NULL::int AS total_agents_count,"

    # is_new: первый показ С АПРЕЛЯ 2025 попал в последние 30 дней
    new_subq = f"""(SELECT MIN(f2.stage_date) FROM mart.deals_funnel_events f2
        WHERE f2.is_test=FALSE AND f2.sales_department='Агентский'
          AND COALESCE(f2.agency_name,'')!='Nedvex'
          AND f2.stage_date >= '2025-04-01'::date
          AND f2.stage_date > '1900-01-01'::date
          AND f2.stage_name='Показ' AND f2.{eid.replace('f.','')}={eid})"""

    return f"""
    WITH base AS (
        SELECT
            {eid} AS entity_id, {enm} AS entity_name,
            f.agency_id, f.agency_name, {pfield},
            COUNT(DISTINCT CASE WHEN f.stage_name='Показ' THEN f.deal_id END) AS total_shows,
            MAX(CASE WHEN f.stage_name='Показ' THEN f.stage_date END) AS last_show_dt,
            CURRENT_DATE - MAX(CASE WHEN f.stage_name='Показ' THEN f.stage_date END) AS days_since_last_show,
            COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END) AS total_deals,
            MAX(CASE WHEN f.stage_name='Сделка' THEN f.stage_date END) AS last_deal_dt,
            CURRENT_DATE - MAX(CASE WHEN f.stage_name='Сделка' THEN f.stage_date END) AS days_since_last_deal,
            SUM(CASE WHEN f.stage_name='Сделка' THEN f.deal_amount ELSE 0 END) AS total_deal_amount,
            CASE WHEN COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END)>0
                THEN SUM(CASE WHEN f.stage_name='Сделка' THEN f.deal_amount ELSE 0 END)
                     /COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END) END AS avg_deal_amount,
            {acnt}
            MAX(CASE WHEN f.stage_name='Показ' THEN f.stage_date END) >= CURRENT_DATE - INTERVAL '90 days' AS is_active_shows,
            {new_subq} >= CURRENT_DATE - INTERVAL '30 days' AS is_new,
            (COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END) > 0
             AND MAX(CASE WHEN f.stage_name='Сделка' THEN f.stage_date END) < CURRENT_DATE - INTERVAL '90 days') AS is_churned,
            CASE WHEN COUNT(DISTINCT CASE WHEN f.stage_name='Показ' THEN f.deal_id END)>0
                THEN ROUND(COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END)::NUMERIC
                     /COUNT(DISTINCT CASE WHEN f.stage_name='Показ' THEN f.deal_id END)*100,1) END AS cr_show_to_deal
        FROM mart.deals_funnel_events f
        {pjoin}
        WHERE {BASE} AND {eid} IS NOT NULL {DATE_FILTER}
        GROUP BY {eid}, {enm}, f.agency_id, f.agency_name {("," + pfield) if entity=="agent" else ""}
    )"""


@app.get("/api/churn/kpi")
async def churn_kpi(entity: str = Query("agent")):
    cte = churn_cte(entity)
    r = qdb(f"""{cte}
        SELECT COUNT(*) FILTER (WHERE is_active_shows) AS active_count,
               COUNT(*) FILTER (WHERE is_new) AS new_count,
               COUNT(*) FILTER (WHERE is_churned) AS churned_count,
               COUNT(*) AS total_count,
               ROUND(AVG(cr_show_to_deal) FILTER (WHERE is_active_shows),1) AS avg_cr,
               ROUND(AVG(active_agents_count) FILTER (WHERE is_active_shows),1) AS avg_agents
        FROM base
    """)
    return jr(r[0] if r else {})


@app.get("/api/churn/pareto")
async def churn_pareto(entity: str = Query("agent")):
    cte = churn_cte(entity)
    segs = [("A: 5+ сделок",5,9999),("B: 2–4 сделки",2,4),("C: 1 сделка",1,1),("D: 0 сделок",0,0)] if entity=="agent" else [("A: 10+ сделок",10,9999),("B: 4–9 сделок",4,9),("C: 1–3 сделки",1,3),("D: 0 сделок",0,0)]
    r = qdb(f"""{cte} SELECT total_deals, COUNT(*) AS cnt FROM base GROUP BY total_deals""")
    dm = {row["total_deals"]: row["cnt"] for row in r}
    res = []
    for label, mn, mx in segs:
        ec = sum(v for k, v in dm.items() if mn <= k <= mx)
        dc = sum(k * v for k, v in dm.items() if mn <= k <= mx)
        res.append({"label": label, "entity_count": ec, "deal_count": dc})
    td = sum(s["deal_count"] for s in res)
    for s in res: s["deal_pct"] = round(s["deal_count"]/td*100, 1) if td > 0 else 0
    return jr(res)


@app.get("/api/churn/top")
async def churn_top(entity: str = Query("agent"), limit: int = Query(10)):
    cte = churn_cte(entity)
    extra = ", active_agents_count" if entity == "agency" else ""
    r = qdb(f"""{cte}
        SELECT entity_id, entity_name, agency_name, total_shows, total_deals,
               cr_show_to_deal, total_deal_amount, agent_phone {extra}
        FROM base WHERE total_shows > 0
        ORDER BY total_deals DESC, total_shows DESC LIMIT %(l)s
    """, {"l": limit})
    return jr(r)


@app.get("/api/churn/risk")
async def churn_risk(entity: str = Query("agent"), page: int = Query(1, ge=1), per_page: int = Query(20)):
    cte = churn_cte(entity)
    total = qdb(f"""{cte} SELECT COUNT(*) AS cnt FROM base WHERE is_churned = TRUE""")
    r = qdb(f"""{cte}
        SELECT entity_id, entity_name, agency_name, last_deal_dt, total_deals,
               days_since_last_deal, total_deal_amount, active_agents_count, agent_phone
        FROM base WHERE is_churned = TRUE
        ORDER BY days_since_last_deal DESC
        OFFSET %(offset)s LIMIT %(limit)s
    """, {"offset": (page-1)*per_page, "limit": per_page})
    return jr({"total": total[0]["cnt"] if total else 0, "page": page, "per_page": per_page, "data": r})


@app.get("/api/churn/risk/excel")
async def churn_risk_excel(entity: str = Query("agent")):
    import openpyxl
    cte = churn_cte(entity)
    r = qdb(f"""{cte}
        SELECT entity_name, agency_name, last_deal_dt, total_deals,
               days_since_last_deal, total_deal_amount, agent_phone
        FROM base WHERE is_churned = TRUE ORDER BY days_since_last_deal DESC
    """)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Зона риска"
    ws.append(["Имя","Агентство","Последняя сделка","Сделок","Дней неактивности","Сумма сделок","Телефон"])
    for row in r:
        ws.append([row["entity_name"],row["agency_name"],str(row["last_deal_dt"]) if row["last_deal_dt"] else "",row["total_deals"],row["days_since_last_deal"],float(row["total_deal_amount"]) if row["total_deal_amount"] else 0,row["agent_phone"] or ""])
    for col in ws.columns:
        mx = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(mx+2, 40)
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            headers={"Content-Disposition": f"attachment; filename=risk_{entity}s.xlsx"})


# ═══════════════════════════════════
# RF (agents: no agency grouping)
# ═══════════════════════════════════

def rf_cte(entity, metric):
    stage = "Сделка" if metric == "deals" else "Показ"

    if entity == "agent":
        # Агенты: НЕ группируем по АН, берём последнее АН
        pjoin = """LEFT JOIN (
            SELECT DISTINCT ON (agent_id) agent_id, agent_phone
            FROM mart.deals WHERE agent_phone IS NOT NULL AND agent_phone!=''
            ORDER BY agent_id, deal_dt DESC
        ) ph ON f.agent_id=ph.agent_id"""

        select_fields = """f.agent_id AS entity_id, f.agent_name AS entity_name,
            (ARRAY_AGG(f.agency_id ORDER BY f.stage_date DESC))[1] AS agency_id,
            (ARRAY_AGG(f.agency_name ORDER BY f.stage_date DESC))[1] AS agency_name,
            ph.agent_phone"""
        group_by = "f.agent_id, f.agent_name, ph.agent_phone"
    else:
        pjoin = ""
        select_fields = """f.agency_id AS entity_id, f.agency_name AS entity_name,
            f.agency_id, f.agency_name, NULL::text AS agent_phone"""
        group_by = "f.agency_id, f.agency_name"

    # F/R пороги
    if metric == "deals":
        fc = "CASE WHEN cnt>=5 THEN 'vip' WHEN cnt>=3 THEN 'top' WHEN cnt>=2 THEN 'loyal' ELSE 'newcomer' END"
        rc = "CASE WHEN rec<=90 THEN 'active' WHEN rec<=180 THEN 'cooling' WHEN rec<=365 THEN 'leaving' ELSE 'gone' END"
    elif entity == "agent":
        fc = "CASE WHEN cnt>=5 THEN 'vip' WHEN cnt>=3 THEN 'top' WHEN cnt>=2 THEN 'loyal' ELSE 'newcomer' END"
        rc = "CASE WHEN rec<=30 THEN 'active' WHEN rec<=90 THEN 'cooling' WHEN rec<=150 THEN 'leaving' ELSE 'gone' END"
    else:
        fc = "CASE WHEN cnt>=16 THEN 'vip' WHEN cnt>=10 THEN 'top' WHEN cnt>=4 THEN 'loyal' ELSE 'newcomer' END"
        rc = "CASE WHEN rec<=30 THEN 'active' WHEN rec<=90 THEN 'cooling' WHEN rec<=150 THEN 'leaving' ELSE 'gone' END"

    eid_filter = "f.agent_id IS NOT NULL" if entity == "agent" else "f.agency_id IS NOT NULL"

    return f"""
    WITH raw_data AS (
        SELECT {select_fields},
               COUNT(DISTINCT f.deal_id) AS cnt,
               CURRENT_DATE - MAX(f.stage_date) AS rec,
               MAX(f.stage_date) AS last_event_dt,
               MIN(f.stage_date) AS first_event_dt,
               SUM(f.deal_amount) AS total_amount,
               AVG(f.deal_amount) AS avg_amount
        FROM mart.deals_funnel_events f
        {pjoin}
        WHERE {BASE} AND {eid_filter} AND f.stage_name=%(stage)s {DATE_FILTER}
        GROUP BY {group_by}
    ),
    base AS (SELECT *, {fc} AS f_segment, {rc} AS r_segment FROM raw_data)
    """, {"stage": stage}


@app.get("/api/rf/kpi")
async def rf_kpi(entity: str = Query("agent"), metric: str = Query("deals")):
    cte, dp = rf_cte(entity, metric)
    r = qdb(f"""{cte}
        SELECT COUNT(*) AS total_entities, SUM(cnt) AS total_events,
               SUM(total_amount) AS total_amount, ROUND(AVG(avg_amount)) AS avg_amount,
               ROUND(COUNT(*) FILTER (WHERE f_segment IN ('vip','top'))::NUMERIC/NULLIF(COUNT(*),0)*100,1) AS pct_vip_top
        FROM base""", dp)
    return jr(r[0] if r else {})


@app.get("/api/rf/matrix")
async def rf_matrix(entity: str = Query("agent"), metric: str = Query("deals")):
    cte, dp = rf_cte(entity, metric)
    r = qdb(f"""{cte} SELECT f_segment, r_segment, COUNT(*) AS cnt FROM base GROUP BY f_segment, r_segment""", dp)
    return jr(r)


@app.get("/api/rf/table")
async def rf_table(entity: str = Query("agent"), metric: str = Query("deals"), f_segment: str = Query(None), r_segment: str = Query(None), limit: int = Query(100)):
    cte, dp = rf_cte(entity, metric)
    filters = []
    if f_segment: filters.append("f_segment=%(fs)s"); dp["fs"] = f_segment
    if r_segment: filters.append("r_segment=%(rs)s"); dp["rs"] = r_segment
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    dp["l"] = limit
    r = qdb(f"""{cte}
        SELECT entity_id, entity_name, agency_name, cnt AS frequency, avg_amount, total_amount,
               last_event_dt, rec AS recency_days, f_segment, r_segment, agent_phone
        FROM base {where}
        ORDER BY cnt DESC, total_amount DESC NULLS LAST LIMIT %(l)s""", dp)
    return jr(r)


@app.get("/api/rf/detail")
async def rf_detail(entity: str = Query("agent"), entity_id: int = Query(None), metric: str = Query("deals"), limit: int = Query(50)):
    idf = "agent_id" if entity == "agent" else "agency_id"
    nmf = "agent_name" if entity == "agent" else "agency_name"
    stage = "Сделка" if metric == "deals" else "Показ"
    c = [BASE, f"{idf} IS NOT NULL", "f.stage_name=%(stage)s", f"f.stage_date >= '{START_DATE}'::date"]
    p = {"stage": stage, "l": limit}
    if entity_id: c.append(f"{idf}=%(eid)s"); p["eid"] = entity_id
    w = " AND ".join(c)
    r = qdb(f"""SELECT {nmf} AS entity_name, agency_name, deal_id, stage_date AS event_dt,
               deal_amount, object_name, 'https://bitned.ru/crm/deal/details/'||deal_id||'/' AS deal_link
        FROM mart.deals_funnel_events f WHERE {w} ORDER BY stage_date DESC LIMIT %(l)s""", p)
    return jr(r)


# ═══════════════════════════════════
# COHORT ANALYSIS
# ═══════════════════════════════════

@app.get("/api/cohort")
async def cohort(entity: str = Query("agent"), metric: str = Query("retention")):
    """
    metric:
      retention — % агентов из когорты с показом в месяце +N
      conversion — % агентов из когорты с сделкой в месяце +N
    """
    eid = "agent_id" if entity == "agent" else "agency_id"
    event_stage = "Показ" if metric == "retention" else "Сделка"

    r = qdb(f"""
    WITH cohorts AS (
        -- Когорта = месяц первого показа С АПРЕЛЯ 2025
        SELECT {eid} AS eid,
               DATE_TRUNC('month', MIN(stage_date))::date AS cohort_month
        FROM mart.deals_funnel_events f
        WHERE {BASE} AND {eid} IS NOT NULL AND stage_name = 'Показ'
              AND stage_date >= '2025-04-01'::date
        GROUP BY {eid}
    ),
    events AS (
        SELECT DISTINCT {eid} AS eid,
               DATE_TRUNC('month', stage_date)::date AS event_month
        FROM mart.deals_funnel_events f
        WHERE {BASE} AND {eid} IS NOT NULL AND stage_name = %(stage)s
              AND stage_date >= '2025-04-01'::date
    ),
    cohort_events AS (
        SELECT c.cohort_month,
               (EXTRACT(YEAR FROM e.event_month) - EXTRACT(YEAR FROM c.cohort_month)) * 12
               + EXTRACT(MONTH FROM e.event_month) - EXTRACT(MONTH FROM c.cohort_month) AS month_offset,
               COUNT(DISTINCT e.eid) AS active_count
        FROM cohorts c
        JOIN events e ON c.eid = e.eid
        WHERE e.event_month >= c.cohort_month
        GROUP BY c.cohort_month, month_offset
    ),
    cohort_sizes AS (
        SELECT cohort_month, COUNT(*) AS cohort_size
        FROM cohorts GROUP BY cohort_month
    )
    SELECT ce.cohort_month,
           cs.cohort_size,
           ce.month_offset,
           ce.active_count,
           ROUND(ce.active_count::numeric / cs.cohort_size * 100, 1) AS pct
    FROM cohort_events ce
    JOIN cohort_sizes cs ON ce.cohort_month = cs.cohort_month
    WHERE ce.month_offset >= 0 AND ce.month_offset <= 12
    ORDER BY ce.cohort_month, ce.month_offset
    """, {"stage": event_stage})
    return jr(r)


@app.get("/api/cohort/detail")
async def cohort_detail(entity: str = Query("agent"), cohort_month: str = Query(...)):
    """Список агентов/АН в когорте с их метриками."""
    eid = "agent_id" if entity == "agent" else "agency_id"
    enm = "agent_name" if entity == "agent" else "agency_name"

    pjoin = ""
    pfield = "NULL::text AS agent_phone"
    if entity == "agent":
        pjoin = f"""LEFT JOIN (
            SELECT DISTINCT ON (agent_id) agent_id, agent_phone
            FROM mart.deals WHERE agent_phone IS NOT NULL AND agent_phone!=''
            ORDER BY agent_id, deal_dt DESC
        ) ph ON f.{eid}=ph.agent_id"""
        pfield = "ph.agent_phone"

    r = qdb(f"""
    WITH cohort_members AS (
        SELECT {eid} AS eid
        FROM mart.deals_funnel_events f
        WHERE {BASE} AND {eid} IS NOT NULL AND stage_name='Показ'
              AND stage_date >= '2025-04-01'::date
        GROUP BY {eid}
        HAVING DATE_TRUNC('month', MIN(stage_date))::date = %(cm)s::date
    )
    SELECT f.{eid} AS entity_id, f.{enm} AS entity_name,
           (ARRAY_AGG(f.agency_name ORDER BY f.stage_date DESC))[1] AS agency_name,
           {pfield},
           COUNT(DISTINCT CASE WHEN f.stage_name='Показ' THEN f.deal_id END) AS total_shows,
           COUNT(DISTINCT CASE WHEN f.stage_name='Сделка' THEN f.deal_id END) AS total_deals,
           MIN(CASE WHEN f.stage_name='Показ' THEN f.stage_date END) AS first_show,
           MAX(CASE WHEN f.stage_name='Показ' THEN f.stage_date END) AS last_show,
           MAX(CASE WHEN f.stage_name='Сделка' THEN f.stage_date END) AS last_deal,
           SUM(CASE WHEN f.stage_name='Сделка' THEN f.deal_amount ELSE 0 END) AS total_amount
    FROM mart.deals_funnel_events f
    {pjoin}
    WHERE {BASE} AND f.{eid} IS NOT NULL
      AND f.{eid} IN (SELECT eid FROM cohort_members)
      AND f.stage_date >= '2025-04-01'::date
    GROUP BY f.{eid}, f.{enm} {("," + pfield) if entity=="agent" else ""}
    ORDER BY total_deals DESC, total_shows DESC
    """, {"cm": cohort_month})
    return jr(r)

# ═══════════════════════════════════
# RATING (Рейтинг агентов/агентств)
# ═══════════════════════════════════

@app.get("/api/rating/agent")
async def rating_agent():
    r = qdb("""SELECT rating, agent AS name, sum_sale, count_deal
               FROM mart.rang_agent ORDER BY rating""")
    return jr(r)

@app.get("/api/rating/agency")
async def rating_agency():
    r = qdb("""SELECT rating, an AS name, sum_sale, count_deal
               FROM mart.rang_an ORDER BY rating""")
    return jr(r)
if __name__ == "__main__":
    import uvicorn
    api_host = os.getenv("API_HOST")
    api_port = int(os.getenv("API_PORT"))
    print("="*50+f"\n  AgentHub v4 (с {START_DATE}) → http://{api_host}:{api_port}\n"+"="*50)
    uvicorn.run(app, host=api_host, port=api_port)
