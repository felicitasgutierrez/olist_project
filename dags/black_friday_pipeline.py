"""
Black Friday Pipeline - Monitoreo de ventas en tiempo real
Extrae de PostgreSQL (tiendas) + API (app), transforma, carga en DWH.
Micro-batch cada 1 minuto. CDC basado en last_updated.
"""
import logging
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuración de logging
logger = logging.getLogger(__name__)

# Constantes
API_SALES_URL = "http://api-sales:8000/sales"
POSTGRES_SOURCE_CONN = "postgres_source"
POSTGRES_DWH_CONN = "postgres_dwh"

default_args = {
    "retries": 3,
    "retry_delay": 60,
    "retry_exponential_backoff": True,
    "max_retry_delay": 300,
}


def _seed_new_sales(**context) -> None:
    """Simula nuevas ventas en tiendas para que el CDC tenga datos cada minuto."""
    import random
    from datetime import datetime

    hook = PostgresHook(postgres_conn_id=POSTGRES_SOURCE_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()
    ts = datetime.now()
    for i in range(random.randint(1, 3)):
        sale_id = f"STORE-{int(ts.timestamp() * 1000) + i}"
        amount = round(random.uniform(49.99, 499.99), 2)
        promo = random.random() > 0.5
        cur.execute(
            """
            INSERT INTO sales_stores (sale_id, sale_timestamp, amount, channel, promotion_used, promotion_code, store_id, product_category, last_updated)
            VALUES (%s, %s, %s, 'physical_store', %s, %s, 'S001', 'Electronics', CURRENT_TIMESTAMP)
            ON CONFLICT (sale_id) DO NOTHING
            """,
            (sale_id, ts, amount, promo, f"BF2024-{random.randint(10, 30)}" if promo else None),
        )
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Ventas simuladas insertadas en tiendas")


def _extract_stores(**context) -> list[dict]:
    """Extrae ventas de PostgreSQL (tiendas) con CDC por last_updated."""
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    # Incluimos buffer de 1 min para capturar datos insertados durante la ejecución (seed)
    window_end = data_interval_end + timedelta(minutes=1)
    logger.info(f"Extrayendo tiendas CDC: {data_interval_start} -> {window_end}")

    hook = PostgresHook(postgres_conn_id=POSTGRES_SOURCE_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sale_id, sale_timestamp, amount, channel, promotion_used, promotion_code, last_updated
        FROM sales_stores
        WHERE last_updated >= %s AND last_updated < %s
        """,
        (data_interval_start, window_end),
    )
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info(f"Extraídas {len(rows)} ventas de tiendas")
    return rows


def _extract_app(**context) -> list[dict]:
    """Extrae ventas de la API de la app."""
    logger.info("Extrayendo ventas de API app")
    try:
        r = requests.get(API_SALES_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.error(f"Error extrayendo API: {e}")
        raise
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    window_start = data_interval_start - timedelta(minutes=2)
    window_end = data_interval_end + timedelta(minutes=1)
    filtered = []
    for row in data:
        ts_str = row.get("sale_timestamp")
        if ts_str:
            from dateutil import parser
            ts = parser.parse(ts_str)
            if window_start <= ts < window_end:
                filtered.append(row)
    logger.info(f"Extraídas {len(filtered)} ventas de app (ventana {window_start} - {window_end})")
    return filtered


def _normalize_row(row: dict, source: str) -> dict:
    """Normaliza una fila al esquema unificado."""
    from dateutil import parser
    ts = row.get("sale_timestamp")
    if isinstance(ts, str):
        ts = parser.parse(ts)
    return {
        "sale_id": str(row.get("sale_id", "")),
        "sale_timestamp": ts,
        "amount": float(row.get("amount", 0)),
        "channel": str(row.get("channel", "unknown")).lower(),
        "promotion_used": bool(row.get("promotion_used", False)),
        "source_system": source,
    }


def _transform_unify(**context) -> list[dict]:
    """Unifica, normaliza y aplica control de calidad (sin montos negativos)."""
    ti = context["ti"]
    stores = ti.xcom_pull(task_ids="extract_stores") or []
    app = ti.xcom_pull(task_ids="extract_app") or []

    unified = []
    for r in stores:
        nr = _normalize_row(r, "physical_store")
        if nr["amount"] < 0:
            logger.warning(f"Control calidad: monto negativo descartado: {nr}")
            continue
        unified.append(nr)
    for r in app:
        nr = _normalize_row(r, "app")
        if nr["amount"] < 0:
            logger.warning(f"Control calidad: monto negativo descartado: {nr}")
            continue
        unified.append(nr)

    logger.info(f"Unificadas {len(unified)} ventas válidas")
    # Serializar timestamps para XCom
    for u in unified:
        u["sale_timestamp"] = u["sale_timestamp"].isoformat()
    return unified


def _load_and_aggregate(**context) -> None:
    """Carga en staging, calcula métricas y persiste en sales_realtime_metrics."""
    ti = context["ti"]
    unified = ti.xcom_pull(task_ids="transform_unify") or []
    data_interval_end = context["data_interval_end"]
    metric_minute = data_interval_end.replace(second=0, microsecond=0)

    if not unified:
        logger.info("Sin datos para cargar en este batch")
        return

    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()

    for u in unified:
        from dateutil import parser
        ts = parser.parse(u["sale_timestamp"])
        cur.execute(
            """
            INSERT INTO stg_sales_unified (sale_id, sale_timestamp, amount, channel, promotion_used, source_system, batch_minute)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING
            """,
            (
                u["sale_id"],
                ts,
                u["amount"],
                u["channel"],
                u["promotion_used"],
                u["source_system"],
                metric_minute,
            ),
        )

    # Calcular métricas
    total_sales = sum(u["amount"] for u in unified)
    sales_count = len(unified)
    sales_per_minute = total_sales  # en este minuto
    promo_count = sum(1 for u in unified if u["promotion_used"])
    promo_pct = (promo_count / sales_count * 100) if sales_count else 0
    avg_ticket = total_sales / sales_count if sales_count else 0

    sales_physical = sum(u["amount"] for u in unified if u["source_system"] == "physical_store")
    sales_app = sum(u["amount"] for u in unified if u["source_system"] == "app")

    cur.execute(
        """
        INSERT INTO sales_realtime_metrics
        (metric_minute, total_sales, sales_count, sales_per_minute, sales_physical_store, sales_app,
         promo_usage_percent, avg_ticket)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (metric_minute) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            sales_count = EXCLUDED.sales_count,
            sales_per_minute = EXCLUDED.sales_per_minute,
            sales_physical_store = EXCLUDED.sales_physical_store,
            sales_app = EXCLUDED.sales_app,
            promo_usage_percent = EXCLUDED.promo_usage_percent,
            avg_ticket = EXCLUDED.avg_ticket,
            batch_loaded_at = CURRENT_TIMESTAMP
        """,
        (metric_minute, total_sales, sales_count, sales_per_minute, sales_physical, sales_app, promo_pct, avg_ticket),
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Métricas cargadas para {metric_minute}: total={total_sales}, count={sales_count}")


def _quality_check(**context) -> None:
    """Valida que no haya montos negativos en el DWH."""
    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stg_sales_unified WHERE amount < 0")
    neg = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sales_realtime_metrics WHERE total_sales < 0")
    neg_agg = cur.fetchone()[0]
    cur.close()
    conn.close()
    if neg > 0 or neg_agg > 0:
        raise ValueError(f"Control de calidad fallido: montos negativos en staging={neg}, metrics={neg_agg}")
    logger.info("Control de calidad OK: sin montos negativos")


with DAG(
    dag_id="black_friday_pipeline",
    start_date=datetime(2024, 11, 1),
    schedule="*/1 * * * *",  # cada 1 minuto
    catchup=False,
    default_args=default_args,
    tags=["black_friday", "etl", "realtime"],
) as dag:

    validate_source = PostgresOperator(
        task_id="validate_source",
        postgres_conn_id=POSTGRES_SOURCE_CONN,
        sql="SELECT 1",
    )

    validate_dwh = PostgresOperator(
        task_id="validate_dwh",
        postgres_conn_id=POSTGRES_DWH_CONN,
        sql="SELECT 1",
    )

    seed_sales = PythonOperator(
        task_id="seed_sales",
        python_callable=_seed_new_sales,
    )

    extract_stores = PythonOperator(
        task_id="extract_stores",
        python_callable=_extract_stores,
    )

    extract_app = PythonOperator(
        task_id="extract_app",
        python_callable=_extract_app,
    )

    transform_unify = PythonOperator(
        task_id="transform_unify",
        python_callable=_transform_unify,
    )

    load_aggregate = PythonOperator(
        task_id="load_aggregate",
        python_callable=_load_and_aggregate,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    # Dependencias
    [validate_source, validate_dwh] >> seed_sales
    seed_sales >> [extract_stores, extract_app]
    [extract_stores, extract_app] >> transform_unify
    transform_unify >> load_aggregate >> quality_check
