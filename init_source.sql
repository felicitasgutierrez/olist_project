-- =============================================
-- Base de datos SOURCE: Tiendas físicas
-- Simula ventas de tiendas físicas con CDC (last_updated)
-- =============================================

-- Tabla de ventas de tiendas físicas
CREATE TABLE IF NOT EXISTS sales_stores (
    id SERIAL PRIMARY KEY,
    sale_id VARCHAR(50) UNIQUE NOT NULL,
    sale_timestamp TIMESTAMP NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    channel VARCHAR(20) DEFAULT 'physical_store',
    promotion_used BOOLEAN DEFAULT FALSE,
    promotion_code VARCHAR(50),
    store_id VARCHAR(20),
    product_category VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice para CDC (extracción por last_updated)
CREATE INDEX IF NOT EXISTS idx_sales_stores_last_updated ON sales_stores(last_updated);

-- Insertar datos de ejemplo para Black Friday
INSERT INTO sales_stores (sale_id, sale_timestamp, amount, channel, promotion_used, promotion_code, store_id, product_category, last_updated)
VALUES
    ('STORE-001', NOW() - INTERVAL '2 minutes', 299.99, 'physical_store', true, 'BF2024-10', 'S001', 'Electronics', NOW()),
    ('STORE-002', NOW() - INTERVAL '2 minutes', 149.50, 'physical_store', false, NULL, 'S001', 'Accessories', NOW()),
    ('STORE-003', NOW() - INTERVAL '1 minute', 899.00, 'physical_store', true, 'BF2024-20', 'S002', 'Electronics', NOW()),
    ('STORE-004', NOW() - INTERVAL '1 minute', 45.99, 'physical_store', false, NULL, 'S002', 'Accessories', NOW()),
    ('STORE-005', NOW(), 1299.99, 'physical_store', true, 'BF2024-15', 'S001', 'Electronics', NOW())
ON CONFLICT (sale_id) DO NOTHING;
