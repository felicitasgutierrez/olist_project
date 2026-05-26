-- =============================================
-- init_db.sql - Documentación de esquemas
-- Los scripts reales están en init_source.sql e init_dwh.sql
-- y se montan en los respectivos contenedores PostgreSQL.
-- =============================================

-- SOURCE (postgres-source / stores_sales):
-- Tabla sales_stores con: id, sale_id, sale_timestamp, amount,
-- channel, promotion_used, promotion_code, store_id, product_category, last_updated

-- DWH (postgres-dwh / dwh):
-- stg_sales_unified: staging de ventas unificadas
-- sales_realtime_metrics: métricas agregadas por minuto
