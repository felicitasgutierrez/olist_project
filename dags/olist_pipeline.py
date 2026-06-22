"""
Olist Analytics Pipeline
========================
Pipeline batch para el procesamiento de datos históricos del marketplace Olist.
Fuente de datos: CSVs estáticos en /opt/airflow/data/input/
Destino: PostgreSQL DWH con tablas de staging + tablas agregadas para Metabase.

Flujo:
  discover_inputs → validate_schema → read_files
  → clean_transform → load_to_dwh
  → build_aggregations → quality_checks

Estrategia de ingesta: batch idempotente (UPSERT por order_id).
El DAG puede ejecutarse manualmente o en schedule diario.
"""

import logging
import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ─── Constantes ────────────────────────────────────────────────────────────────

POSTGRES_DWH_CONN = "postgres_dwh"
DATA_INPUT_PATH = "/opt/airflow/data/input"

# Archivos requeridos y sus columnas esperadas
REQUIRED_FILES = {
    "orders_dataset.csv": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items_dataset.csv": [
        "order_id", "order_item_id", "product_id", "seller_id",
        "price", "freight_value",
    ],
    "customers_dataset.csv": [
        "customer_id", "customer_unique_id",
        "customer_city", "customer_state",
    ],
    "products_dataset.csv": [
        "product_id", "product_category_name",
    ],
    "order_reviews_dataset.csv": [
        "review_id", "order_id", "review_score",
    ],
}

# Archivos opcionales (no rompen el pipeline si no existen)
OPTIONAL_FILES = {
    "sellers_dataset.csv": [
        "seller_id", "seller_city", "seller_state",
    ],
    "order_payments_dataset.csv": [
        "order_id", "payment_type", "payment_value",
    ],
    "product_category_name_translation.csv": [
        "product_category_name", "product_category_name_english",
    ],
}


# ─── Tarea 1: discover_inputs ──────────────────────────────────────────────────

def _discover_inputs(**context) -> dict:
    """
    Verifica que los CSVs requeridos de Olist estén en data/input/.
    Si falta algún archivo crítico, falla y avisa antes de procesar nada.
    Retorna un dict con los paths encontrados para cada archivo.
    """
    logger.info(f"Buscando archivos en {DATA_INPUT_PATH}")

    if not os.path.isdir(DATA_INPUT_PATH):
        raise FileNotFoundError(
            f"Directorio de entrada no encontrado: {DATA_INPUT_PATH}. "
            "Asegurate de tener los CSVs de Olist en data/input/"
        )

    found = {}
    missing = []

    for filename in REQUIRED_FILES:
        full_path = os.path.join(DATA_INPUT_PATH, filename)
        if os.path.isfile(full_path):
            size_mb = os.path.getsize(full_path) / (1024 * 1024)
            found[filename] = full_path
            logger.info(f"  ✓ {filename} ({size_mb:.1f} MB)")
        else:
            missing.append(filename)
            logger.error(f"  ✗ {filename} — NO ENCONTRADO")

    if missing:
        raise FileNotFoundError(
            f"Archivos requeridos faltantes: {missing}. "
            f"Colocá los CSVs de Olist en {DATA_INPUT_PATH}"
        )

    for filename in OPTIONAL_FILES:
        full_path = os.path.join(DATA_INPUT_PATH, filename)
        if os.path.isfile(full_path):
            found[filename] = full_path
            logger.info(f"  ✓ {filename} (opcional)")
        else:
            logger.warning(f"  ~ {filename} no encontrado (opcional, se omite)")

    logger.info(f"discover_inputs OK: {len(found)} archivos disponibles")
    return found


# ─── Tarea 2: validate_schema ─────────────────────────────────────────────────

def _validate_schema(**context) -> None:
    """
    Lee las primeras filas de cada CSV y verifica que tengan las columnas
    esperadas. Detecta archivos corruptos o versiones incorrectas del dataset
    antes de procesar nada.
    """
    ti = context["ti"]
    found = ti.xcom_pull(task_ids="discover_inputs")

    errors = []

    # Validar archivos requeridos
    for filename, expected_cols in REQUIRED_FILES.items():
        if filename not in found:
            continue
        path = found[filename]
        try:
            df_head = pd.read_csv(path, nrows=1)
            actual_cols = set(df_head.columns.tolist())
            missing_cols = set(expected_cols) - actual_cols
            if missing_cols:
                errors.append(
                    f"{filename}: columnas faltantes {missing_cols}"
                )
            else:
                logger.info(f"  ✓ {filename}: schema OK")
        except Exception as e:
            errors.append(f"{filename}: error al leer ({e})")

    # Validar archivos opcionales presentes
    for filename, expected_cols in OPTIONAL_FILES.items():
        if filename not in found:
            continue
        path = found[filename]
        try:
            df_head = pd.read_csv(path, nrows=1)
            actual_cols = set(df_head.columns.tolist())
            missing_cols = set(expected_cols) - actual_cols
            if missing_cols:
                logger.warning(
                    f"  ~ {filename}: columnas opcionales faltantes {missing_cols}"
                )
            else:
                logger.info(f"  ✓ {filename}: schema OK (opcional)")
        except Exception as e:
            logger.warning(f"  ~ {filename}: error al leer ({e}) — se omite")

    if errors:
        raise ValueError(
            f"validate_schema falló con {len(errors)} error(es):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.info("validate_schema OK: todos los schemas son correctos")


# ─── Tarea 3: read_files ──────────────────────────────────────────────────────

def _read_files(**context) -> dict:
    """
    Lee todos los CSVs disponibles con pandas y los serializa para XCom.
    Retorna un dict con los DataFrames convertidos a JSON (orient='split').
    """
    ti = context["ti"]
    found = ti.xcom_pull(task_ids="discover_inputs")

    all_files = {**REQUIRED_FILES, **OPTIONAL_FILES}
    dataframes = {}

    for filename in all_files:
        if filename not in found:
            continue
        path = found[filename]
        try:
            df = pd.read_csv(path, low_memory=False)
            dataframes[filename] = df.to_json(orient="split", date_format="iso")
            logger.info(f"  ✓ {filename}: {len(df):,} filas leídas")
        except Exception as e:
            logger.error(f"  ✗ {filename}: error al leer ({e})")
            raise

    logger.info(f"read_files OK: {len(dataframes)} DataFrames cargados")
    return dataframes


# ─── Tarea 4: clean_transform ─────────────────────────────────────────────────

def _clean_transform(**context) -> str:
    """
    Limpia, normaliza y enriquece los datos:
      - Elimina filas con datos críticos vacíos
      - Unifica formatos de fecha
      - Calcula campos derivados:
          entregado_a_tiempo: comparación entre fecha real y estimada
          ratio_flete: freight_value / price
      - Une todos los CSVs en un único DataFrame de staging
    Retorna el DataFrame final como JSON.
    """
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="read_files")

    # ── Deserializar DataFrames ──
    orders = pd.read_json(raw["orders_dataset.csv"], orient="split")
    items = pd.read_json(raw["order_items_dataset.csv"], orient="split")
    customers = pd.read_json(raw["customers_dataset.csv"], orient="split")
    products = pd.read_json(raw["products_dataset.csv"], orient="split")
    reviews = pd.read_json(raw["order_reviews_dataset.csv"], orient="split")

    logger.info(f"Órdenes crudas: {len(orders):,}")

    # ── Parsear fechas ──
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # ── Filtrar órdenes con datos críticos vacíos ──
    antes = len(orders)
    orders = orders.dropna(subset=["order_id", "customer_id", "order_purchase_timestamp"])
    logger.info(f"Órdenes después de dropna críticos: {len(orders):,} (eliminadas {antes - len(orders):,})")

    # ── Calcular entregado_a_tiempo ──
    # Solo para órdenes con estado 'delivered' y fechas disponibles
    mask_delivered = (
        (orders["order_status"] == "delivered")
        & orders["order_delivered_customer_date"].notna()
        & orders["order_estimated_delivery_date"].notna()
    )
    orders["entregado_a_tiempo"] = None
    orders.loc[mask_delivered, "entregado_a_tiempo"] = (
        orders.loc[mask_delivered, "order_delivered_customer_date"]
        <= orders.loc[mask_delivered, "order_estimated_delivery_date"]
    )

    # ── Agregar items: precio total y flete total por orden ──
    items_agg = (
        items.groupby("order_id")
        .agg(
            precio_total=("price", "sum"),
            flete_total=("freight_value", "sum"),
            cantidad_items=("order_item_id", "count"),
        )
        .reset_index()
    )

    # Calcular ratio_flete (flete relativo al precio)
    items_agg["ratio_flete"] = (
        items_agg["flete_total"] / items_agg["precio_total"].replace(0, float("nan"))
    ).round(4)

    # ── Unir con customers ──
    customers_slim = customers[["customer_id", "customer_unique_id", "customer_city", "customer_state"]].drop_duplicates("customer_id")

    # ── Unir con products (categoría del primer ítem de cada orden) ──
    items_product = items[["order_id", "product_id"]].drop_duplicates("order_id")
    products_slim = products[["product_id", "product_category_name"]].drop_duplicates("product_id")

    # Traducción de categorías (si está disponible)
    translation_key = "product_category_name_translation.csv"
    if translation_key in raw:
        translation = pd.read_json(raw[translation_key], orient="split")
        if "product_category_name" in translation.columns and "product_category_name_english" in translation.columns:
            products_slim = products_slim.merge(
                translation[["product_category_name", "product_category_name_english"]],
                on="product_category_name",
                how="left",
            )
            products_slim["product_category_name"] = (
                products_slim["product_category_name_english"]
                .fillna(products_slim["product_category_name"])
            )
            products_slim = products_slim.drop(columns=["product_category_name_english"], errors="ignore")

    items_with_category = items_product.merge(products_slim, on="product_id", how="left")

    # ── Score promedio de reviews por orden ──
    review_agg = (
        reviews.groupby("order_id")["review_score"]
        .mean()
        .reset_index()
        .rename(columns={"review_score": "review_score_avg"})
    )

    # ── Join final ──
    df = (
        orders
        .merge(items_agg, on="order_id", how="left")
        .merge(customers_slim, on="customer_id", how="left")
        .merge(items_with_category[["order_id", "product_category_name"]], on="order_id", how="left")
        .merge(review_agg, on="order_id", how="left")
    )

    # ── Renombrar y seleccionar columnas finales del staging ──
    staging = df.rename(columns={
        "order_delivered_customer_date": "order_delivered_date",
        "product_category_name": "categoria_producto",
        "review_score_avg": "review_score"
    }).loc[
        :,
        [
            "order_id",
            "customer_id",
            "customer_unique_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_date",
            "order_estimated_delivery_date",
            "entregado_a_tiempo",
            "customer_state",
            "customer_city",
            "precio_total",
            "flete_total",
            "ratio_flete",
            "review_score",
            "categoria_producto",
        ]
    ].rename(columns={"order_purchase_timestamp": "order_purchase_date"})

    # ── Sanitizar valores numéricos ──
    staging["precio_total"] = staging["precio_total"].fillna(0).clip(lower=0)
    staging["flete_total"] = staging["flete_total"].fillna(0).clip(lower=0)
    staging["review_score"] = staging["review_score"].where(
        staging["review_score"].between(1, 5), other=None
    )

    logger.info(f"clean_transform OK: {len(staging):,} filas de staging listas")
    return staging.to_json(orient="split", date_format="iso")


# ─── Tarea 5: load_to_dwh ─────────────────────────────────────────────────────

def _load_to_dwh(**context) -> None:
    """
    Carga el DataFrame de staging limpio en olist_orders_clean (PostgreSQL DWH).
    Usa UPSERT (ON CONFLICT DO UPDATE) para ser idempotente:
    si la orden ya existe, actualiza sus campos en lugar de duplicar.
    """
    ti = context["ti"]
    staging_json = ti.xcom_pull(task_ids="clean_transform")
    staging = pd.read_json(staging_json, orient="split")     
    staging["entregado_a_tiempo"] = staging["entregado_a_tiempo"].map({1.0: True, 0.0: False, 1: True, 0: False}).where(staging["entregado_a_tiempo"].notna(), other=None)

    if staging.empty:
        logger.info("load_to_dwh: sin datos para cargar")
        return

    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO olist_orders_clean (
            order_id, customer_id, customer_unique_id, order_status,
            order_purchase_date, order_delivered_date, order_estimated_delivery_date,
            entregado_a_tiempo, customer_state, customer_city,
            precio_total, flete_total, ratio_flete,
            review_score, categoria_producto
        ) VALUES (
            %(order_id)s, %(customer_id)s, %(customer_unique_id)s, %(order_status)s,
            %(order_purchase_date)s, %(order_delivered_date)s, %(order_estimated_delivery_date)s,
            %(entregado_a_tiempo)s, %(customer_state)s, %(customer_city)s,
            %(precio_total)s, %(flete_total)s, %(ratio_flete)s,
            %(review_score)s, %(categoria_producto)s
        )
        ON CONFLICT (order_id) DO UPDATE SET
            customer_unique_id              = EXCLUDED.customer_unique_id,
            order_status                    = EXCLUDED.order_status,
            order_delivered_date            = EXCLUDED.order_delivered_date,
            order_estimated_delivery_date   = EXCLUDED.order_estimated_delivery_date,
            entregado_a_tiempo              = EXCLUDED.entregado_a_tiempo,
            customer_state                  = EXCLUDED.customer_state,
            customer_city                   = EXCLUDED.customer_city,
            precio_total                    = EXCLUDED.precio_total,
            flete_total                     = EXCLUDED.flete_total,
            ratio_flete                     = EXCLUDED.ratio_flete,
            review_score                    = EXCLUDED.review_score,
            categoria_producto              = EXCLUDED.categoria_producto
    """

    # Convertir NaT/NaN a None para psycopg2
    def _none_if_na(v):
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            return None
        return v

    rows_inserted = 0
    for _, row in staging.iterrows():
        record = {col: _none_if_na(row[col]) for col in staging.columns}
        cur.execute(upsert_sql, record)
        rows_inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"load_to_dwh OK: {rows_inserted:,} filas cargadas/actualizadas en olist_orders_clean")


# ─── Tarea 6: update_rango_dataset ───────────────────────────────────────────

def _update_rango_dataset(**context) -> None:
    """
    Calcula la fecha mínima y máxima de order_purchase_date en olist_orders_clean
    y las guarda en la tabla rango_dataset. Se ejecuta después de load_to_dwh
    para que la referencia del dashboard siempre refleje el dataset cargado.
    """
    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO rango_dataset (id, fecha_min, fecha_max)
        SELECT
            1,
            MIN(order_purchase_date)::DATE,
            MAX(order_purchase_date)::DATE
        FROM olist_orders_clean
        WHERE order_purchase_date IS NOT NULL
          AND order_status NOT IN ('canceled', 'unavailable')
        ON CONFLICT (id) DO UPDATE SET
            fecha_min = EXCLUDED.fecha_min,
            fecha_max = EXCLUDED.fecha_max
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("update_rango_dataset OK: rango de fechas actualizado en el DWH")


# ─── Tarea 7: build_aggregations ──────────────────────────────────────────────

def _build_aggregations(**context) -> None:
    """
    Calcula las métricas del dashboard a partir de olist_orders_clean y las
    guarda en las tres tablas agregadas:
      - kpis_por_mes
      - metricas_por_categoria
      - metricas_por_estado

    Todas las agregaciones se hacen directamente sobre el DWH con SQL
    para evitar traer miles de filas a memoria de Airflow.
    """
    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()

    # ── 1. kpis_por_mes ──────────────────────────────────────────────────────
    logger.info("Calculando kpis_por_mes...")
    cur.execute("""
        INSERT INTO kpis_por_mes (
            mes,
            gmv_total,
            ticket_promedio,
            ticket_mediana,
            pct_entregas_a_tiempo,
            score_promedio_reviews,
            tasa_retencion,
            ratio_flete_gmv,
            cantidad_ordenes
        )
        WITH base AS (
            SELECT
                DATE_TRUNC('month', order_purchase_date)::DATE AS mes,
                order_id,
                customer_id,
                precio_total,
                flete_total,
                entregado_a_tiempo,
                review_score,
                ratio_flete
            FROM olist_orders_clean
            WHERE order_purchase_date IS NOT NULL
              AND order_status NOT IN ('canceled', 'unavailable')
        ),
        -- Tasa de retención: clientes que compraron en el mes actual
        -- que también compraron en algún mes anterior
        clientes_por_mes AS (
            SELECT
                DATE_TRUNC('month', order_purchase_date)::DATE AS mes,
                customer_unique_id,
                MIN(order_purchase_date) AS primera_compra
            FROM olist_orders_clean
            GROUP BY mes, customer_unique_id
        ),
        retencion AS (
            SELECT
                c1.mes,
                COUNT(DISTINCT CASE
                    WHEN c2.customer_unique_id IS NOT NULL THEN c1.customer_unique_id
                END) * 100.0 / NULLIF(COUNT(DISTINCT c1.customer_unique_id), 0) AS tasa_retencion
            FROM clientes_por_mes c1
            LEFT JOIN clientes_por_mes c2
                ON c1.customer_unique_id = c2.customer_unique_id
                AND c2.mes < c1.mes
            GROUP BY c1.mes
        ),
        metricas AS (
            SELECT
                mes,
                SUM(precio_total)                           AS gmv_total,
                AVG(precio_total)                           AS ticket_promedio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY precio_total)                  AS ticket_mediana,
                100.0 * SUM(CASE WHEN entregado_a_tiempo THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN entregado_a_tiempo IS NOT NULL THEN 1 ELSE 0 END), 0)
                                                            AS pct_entregas_a_tiempo,
                AVG(review_score)                           AS score_promedio_reviews,
                SUM(flete_total) / NULLIF(SUM(precio_total), 0) * 100
                                                            AS ratio_flete_gmv,
                COUNT(order_id)                             AS cantidad_ordenes
            FROM base
            GROUP BY mes
        )
        SELECT
            m.mes,
            ROUND(m.gmv_total::NUMERIC, 2)              AS gmv_total,
            ROUND(m.ticket_promedio::NUMERIC, 2)        AS ticket_promedio,
            ROUND(m.ticket_mediana::NUMERIC, 2)         AS ticket_mediana,
            ROUND(m.pct_entregas_a_tiempo::NUMERIC, 2)  AS pct_entregas_a_tiempo,
            ROUND(m.score_promedio_reviews::NUMERIC, 2) AS score_promedio_reviews,
            ROUND(r.tasa_retencion::NUMERIC, 2)         AS tasa_retencion,
            ROUND(m.ratio_flete_gmv::NUMERIC, 2)        AS ratio_flete_gmv,
            m.cantidad_ordenes
        FROM metricas m
        LEFT JOIN retencion r ON m.mes = r.mes
        ON CONFLICT (mes) DO UPDATE SET
            gmv_total               = EXCLUDED.gmv_total,
            ticket_promedio         = EXCLUDED.ticket_promedio,
            ticket_mediana          = EXCLUDED.ticket_mediana,
            pct_entregas_a_tiempo   = EXCLUDED.pct_entregas_a_tiempo,
            score_promedio_reviews  = EXCLUDED.score_promedio_reviews,
            tasa_retencion          = EXCLUDED.tasa_retencion,
            ratio_flete_gmv         = EXCLUDED.ratio_flete_gmv,
            cantidad_ordenes        = EXCLUDED.cantidad_ordenes
    """)

    # ── 2. metricas_por_categoria ─────────────────────────────────────────────
    logger.info("Calculando metricas_por_categoria...")
    cur.execute("""
        INSERT INTO metricas_por_categoria (
            mes,
            categoria,
            gmv_total,
            cantidad_ordenes,
            ticket_promedio
        )
        SELECT
            DATE_TRUNC('month', order_purchase_date)::DATE  AS mes,
            COALESCE(categoria_producto, 'sin_categoria')   AS categoria,
            ROUND(SUM(precio_total)::NUMERIC, 2)            AS gmv_total,
            COUNT(order_id)                                  AS cantidad_ordenes,
            ROUND(AVG(precio_total)::NUMERIC, 2)            AS ticket_promedio
        FROM olist_orders_clean
        WHERE order_purchase_date IS NOT NULL
          AND order_status NOT IN ('canceled', 'unavailable')
        GROUP BY
            DATE_TRUNC('month', order_purchase_date)::DATE,
            COALESCE(categoria_producto, 'sin_categoria')
        ON CONFLICT (mes, categoria) DO UPDATE SET
            gmv_total       = EXCLUDED.gmv_total,
            cantidad_ordenes= EXCLUDED.cantidad_ordenes,
            ticket_promedio = EXCLUDED.ticket_promedio
    """)

    # ── 3. metricas_por_estado ────────────────────────────────────────────────
    logger.info("Calculando metricas_por_estado...")
    cur.execute("""
        INSERT INTO metricas_por_estado (
            mes,
            estado,
            gmv_total,
            cantidad_ordenes,
            pct_entregas_a_tiempo,
            score_promedio_reviews
        )
        SELECT
            DATE_TRUNC('month', order_purchase_date)::DATE  AS mes,
            customer_state                                   AS estado,
            ROUND(SUM(precio_total)::NUMERIC, 2)            AS gmv_total,
            COUNT(order_id)                                  AS cantidad_ordenes,
            ROUND(
                100.0 * SUM(CASE WHEN entregado_a_tiempo THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN entregado_a_tiempo IS NOT NULL THEN 1 ELSE 0 END), 0)
                ::NUMERIC, 2)                               AS pct_entregas_a_tiempo,
            ROUND(AVG(review_score)::NUMERIC, 2)            AS score_promedio_reviews
        FROM olist_orders_clean
        WHERE order_purchase_date IS NOT NULL
          AND customer_state IS NOT NULL
          AND order_status NOT IN ('canceled', 'unavailable')
        GROUP BY
            DATE_TRUNC('month', order_purchase_date)::DATE,
            customer_state
        ON CONFLICT (mes, estado) DO UPDATE SET
            gmv_total               = EXCLUDED.gmv_total,
            cantidad_ordenes        = EXCLUDED.cantidad_ordenes,
            pct_entregas_a_tiempo   = EXCLUDED.pct_entregas_a_tiempo,
            score_promedio_reviews  = EXCLUDED.score_promedio_reviews
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("build_aggregations OK: kpis_por_mes, metricas_por_categoria, metricas_por_estado actualizadas")


# ─── Tarea 7: quality_checks ──────────────────────────────────────────────────

def _quality_checks(**context) -> None:
    """
    Verifica que los resultados del pipeline tienen sentido:
      - Sin precios negativos en staging
      - Sin GMV negativo en kpis_por_mes
      - Sin porcentajes fuera del rango [0, 100]
      - Sin review_score fuera del rango [1, 5]
      - La cantidad total de órdenes en staging es razonable (> 0)
    Si alguna verificación falla, el pipeline falla con un mensaje claro.
    """
    dwh = PostgresHook(postgres_conn_id=POSTGRES_DWH_CONN)
    conn = dwh.get_conn()
    cur = conn.cursor()

    errors = []

    # 1. Sin precios negativos en staging
    cur.execute("SELECT COUNT(*) FROM olist_orders_clean WHERE precio_total < 0")
    neg_precio = cur.fetchone()[0]
    if neg_precio > 0:
        errors.append(f"staging: {neg_precio} filas con precio_total negativo")

    # 2. Sin flete negativo en staging
    cur.execute("SELECT COUNT(*) FROM olist_orders_clean WHERE flete_total < 0")
    neg_flete = cur.fetchone()[0]
    if neg_flete > 0:
        errors.append(f"staging: {neg_flete} filas con flete_total negativo")

    # 3. Sin GMV negativo en kpis_por_mes
    cur.execute("SELECT COUNT(*) FROM kpis_por_mes WHERE gmv_total < 0")
    neg_gmv = cur.fetchone()[0]
    if neg_gmv > 0:
        errors.append(f"kpis_por_mes: {neg_gmv} filas con gmv_total negativo")

    # 4. Porcentajes dentro del rango [0, 100]
    cur.execute("""
        SELECT COUNT(*) FROM kpis_por_mes
        WHERE pct_entregas_a_tiempo IS NOT NULL
          AND (pct_entregas_a_tiempo < 0 OR pct_entregas_a_tiempo > 100)
    """)
    bad_pct = cur.fetchone()[0]
    if bad_pct > 0:
        errors.append(f"kpis_por_mes: {bad_pct} filas con pct_entregas_a_tiempo fuera de [0,100]")

    # 5. Review scores dentro del rango [1, 5]
    cur.execute("""
        SELECT COUNT(*) FROM olist_orders_clean
        WHERE review_score IS NOT NULL
          AND (review_score < 1 OR review_score > 5)
    """)
    bad_reviews = cur.fetchone()[0]
    if bad_reviews > 0:
        errors.append(f"staging: {bad_reviews} filas con review_score fuera de [1,5]")

    # 6. El pipeline procesó al menos una orden
    cur.execute("SELECT COUNT(*) FROM olist_orders_clean")
    total_orders = cur.fetchone()[0]
    if total_orders == 0:
        errors.append("staging: olist_orders_clean está vacía — no se procesó ninguna orden")

    # 7. kpis_por_mes tiene al menos un mes
    cur.execute("SELECT COUNT(*) FROM kpis_por_mes")
    total_kpi_rows = cur.fetchone()[0]
    if total_kpi_rows == 0:
        errors.append("kpis_por_mes está vacía — build_aggregations no generó resultados")

    cur.close()
    conn.close()

    if errors:
        raise ValueError(
            f"quality_checks falló con {len(errors)} error(es):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.info(
        f"quality_checks OK: {total_orders:,} órdenes en staging, "
        f"{total_kpi_rows} meses en kpis_por_mes"
    )


# ─── Definición del DAG ───────────────────────────────────────────────────────

default_args = {
    "retries": 1,
    "retry_delay": 30,
}

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",       # Corre una vez por día; también puede ejecutarse manualmente
    catchup=False,           # No reprocesa días anteriores automáticamente
    default_args=default_args,
    tags=["olist", "etl", "batch"],
    doc_md="""
## Olist Analytics Pipeline

Pipeline batch para el procesamiento de datos del marketplace Olist.

**Flujo:**
```
discover_inputs → validate_schema → read_files
→ clean_transform → load_to_dwh
→ build_aggregations → quality_checks
```

**Para ejecutar manualmente:**
En la UI de Airflow, activar el DAG y hacer click en el botón "Trigger DAG".

**Datos requeridos:**
Colocar los CSVs de Olist Kaggle en `data/input/`:
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_customers_dataset.csv
- olist_products_dataset.csv
- olist_order_reviews_dataset.csv
""",
) as dag:

    discover_inputs = PythonOperator(
        task_id="discover_inputs",
        python_callable=_discover_inputs,
        doc_md="Verifica que los CSVs de Olist están en data/input/",
    )

    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=_validate_schema,
        doc_md="Valida que cada CSV tenga las columnas esperadas",
    )

    read_files = PythonOperator(
        task_id="read_files",
        python_callable=_read_files,
        doc_md="Lee los CSVs con pandas y los pasa al siguiente paso",
    )

    clean_transform = PythonOperator(
        task_id="clean_transform",
        python_callable=_clean_transform,
        doc_md="Limpia, normaliza y une todos los CSVs en un DataFrame de staging",
    )

    load_to_dwh = PythonOperator(
        task_id="load_to_dwh",
        python_callable=_load_to_dwh,
        doc_md="Carga el staging limpio en PostgreSQL DWH con UPSERT",
    )

    update_rango_dataset = PythonOperator(
        task_id="update_rango_dataset",
        python_callable=_update_rango_dataset,
        doc_md="Calcula y guarda el rango de fechas disponible en el dataset",
    )

    build_aggregations = PythonOperator(
        task_id="build_aggregations",
        python_callable=_build_aggregations,
        doc_md="Calcula KPIs: kpis_por_mes, metricas_por_categoria, metricas_por_estado",
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=_quality_checks,
        doc_md="Verifica que los resultados del pipeline tienen sentido",
    )

    # ── Dependencias (flujo lineal) ──
    discover_inputs >> validate_schema >> read_files >> clean_transform >> load_to_dwh >> update_rango_dataset >> build_aggregations >> quality_checks
