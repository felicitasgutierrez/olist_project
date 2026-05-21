-- ============================================================
-- DWH - Olist Analytics Dashboard
-- Ingeniería de Software 2026
-- ============================================================

-- Tabla de staging: detalle limpio, una fila por orden
CREATE TABLE IF NOT EXISTS olist_orders_clean (
    order_id                        VARCHAR PRIMARY KEY,
    customer_id                     VARCHAR NOT NULL,
    order_status                    VARCHAR,
    order_purchase_date             TIMESTAMP,
    order_delivered_date            TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    entregado_a_tiempo              BOOLEAN,
    customer_state                  VARCHAR(2),
    customer_city                   VARCHAR,
    precio_total                    NUMERIC(12, 2),
    flete_total                     NUMERIC(12, 2),
    ratio_flete                     NUMERIC(8, 4),
    review_score                    NUMERIC(3, 1),
    categoria_producto              VARCHAR
);

-- ============================================================
-- Tablas agregadas para el dashboard
-- ============================================================

-- KPIs generales por mes
CREATE TABLE IF NOT EXISTS kpis_por_mes (
    mes                         DATE PRIMARY KEY,
    gmv_total                   NUMERIC(14, 2),
    ticket_promedio             NUMERIC(12, 2),
    ticket_mediana              NUMERIC(12, 2),
    pct_entregas_a_tiempo       NUMERIC(5, 2),
    score_promedio_reviews      NUMERIC(3, 2),
    tasa_retencion              NUMERIC(5, 2),
    ratio_flete_gmv             NUMERIC(5, 2),
    cantidad_ordenes            INTEGER
);

-- Métricas por categoría de producto por mes
CREATE TABLE IF NOT EXISTS metricas_por_categoria (
    mes                 DATE,
    categoria           VARCHAR,
    gmv_total           NUMERIC(14, 2),
    cantidad_ordenes    INTEGER,
    ticket_promedio     NUMERIC(12, 2),
    PRIMARY KEY (mes, categoria)
);

-- Métricas por estado geográfico por mes
-- El campo estado usa las siglas de 2 letras de Brasil (SP, RJ, MG, etc.)
-- que Metabase reconoce automáticamente para el mapa
CREATE TABLE IF NOT EXISTS metricas_por_estado (
    mes                         DATE,
    estado                      VARCHAR(2),
    gmv_total                   NUMERIC(14, 2),
    cantidad_ordenes            INTEGER,
    pct_entregas_a_tiempo       NUMERIC(5, 2),
    score_promedio_reviews      NUMERIC(3, 2),
    PRIMARY KEY (mes, estado)
);
