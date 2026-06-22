# Olist Analytics Pipeline

Pipeline batch orquestado con Apache Airflow para procesar los datos históricos del marketplace brasileño **Olist**. Lee CSVs del dataset público de Kaggle, los limpia y une en un DWH PostgreSQL, y calcula métricas agregadas listas para visualizar en Metabase.

## Inicio rápido

```bash
docker compose up -d --build
```

- **Airflow UI**: http://localhost:8080 (usuario: `admin`, contraseña: `admin`)
- **Metabase**: http://localhost:3000 — login: `admin@blackfriday.local` / `BlackFridayLab1`

---

## Datos de entrada

El pipeline lee los CSVs del [dataset de Olist en Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) desde la carpeta `data/input/`. Los archivos se suben manualmente a esa carpeta antes de ejecutar el DAG.

Si la carpeta no existe, crearla antes de copiar los archivos:

```bash
mkdir -p data/input data/output
```

### Archivos requeridos

| Archivo | Descripción |
|---------|-------------|
| `orders_dataset.csv` | Órdenes con fechas de compra, entrega y estado |
| `order_items_dataset.csv` | Ítems por orden: precio, flete, producto, vendedor |
| `customers_dataset.csv` | Clientes con ciudad y estado |
| `products_dataset.csv` | Productos con categoría |
| `order_reviews_dataset.csv` | Reviews con score por orden |

### Archivo opcional

| Archivo | Descripción |
|---------|-------------|
| `product_category_name_translation.csv` | Traduce los nombres de categoría del portugués al inglés. Si no está presente, las categorías quedan en portugués. |

> `sellers_dataset.csv`, `order_payments_dataset.csv` y `geolocation_dataset.csv` están en la carpeta pero el pipeline no los utiliza actualmente.

---

## DAG: `olist_pipeline`

- **Ejecución**: manual desde la UI de Airflow (Trigger DAG)
- **Estrategia**: batch idempotente con UPSERT por `order_id`; re-ejecutar el DAG no duplica datos

### Flujo de tareas

```
discover_inputs
  >> validate_schema
  >> read_files
  >> clean_transform
  >> load_to_dwh
  >> build_aggregations
  >> quality_checks
```

| Tarea | Qué hace |
|-------|----------|
| `discover_inputs` | Verifica que los CSVs requeridos estén en `data/input/`; falla antes de procesar si falta alguno |
| `validate_schema` | Lee la primera fila de cada CSV y verifica que tenga las columnas esperadas |
| `read_files` | Carga todos los CSVs con pandas y los pasa por XCom |
| `clean_transform` | Parsea fechas, descarta filas con datos críticos vacíos, calcula `entregado_a_tiempo` y `ratio_flete`, une todos los CSVs en un DataFrame de staging |
| `load_to_dwh` | Inserta el staging en `olist_orders_clean` con UPSERT por `order_id` |
| `build_aggregations` | Calcula las tres tablas agregadas para el dashboard directamente en SQL |
| `quality_checks` | Verifica precios no negativos, porcentajes en rango [0,100], reviews en [1,5], y que el staging no esté vacío |

### Ejecución manual

En la UI de Airflow (http://localhost:8080), activar el DAG `olist_pipeline` y hacer clic en **Trigger DAG**.

---

## Arquitectura

```
┌─────────────────────────────────────────┐
│   data/input/  (CSVs de Olist Kaggle)  │
│                                         │
│  orders · order_items · customers       │
│  products · reviews                     │
│  category_translation (opcional)        │
└──────────────────┬──────────────────────┘
                   │  lectura directa
                   ▼
┌─────────────────────────────────────────┐
│         Airflow — olist_pipeline        │
│                                         │
│  discover → validate → read             │
│  → clean_transform                      │
│  → load_to_dwh                          │
│  → build_aggregations                   │
│  → quality_checks                       │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│          postgres-dwh                   │
│                                         │
│  olist_orders_clean  (staging)          │
│  kpis_por_mes                           │
│  metricas_por_categoria                 │
│  metricas_por_estado                    │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│              Metabase                   │
│         (dashboards y reportes)         │
└─────────────────────────────────────────┘
```

---

## Tablas del DWH

### `olist_orders_clean` — staging

Una fila por orden. Es la fuente de todas las agregaciones.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `order_id` | VARCHAR PK | ID de la orden |
| `customer_id` | VARCHAR | ID del cliente (instancia) |
| `customer_unique_id` | VARCHAR | ID del cliente único (para retención) |
| `order_status` | VARCHAR | Estado de la orden |
| `order_purchase_date` | TIMESTAMP | Fecha de compra |
| `order_delivered_date` | TIMESTAMP | Fecha de entrega real |
| `order_estimated_delivery_date` | TIMESTAMP | Fecha de entrega estimada |
| `entregado_a_tiempo` | BOOLEAN | Entregado antes o en la fecha estimada |
| `customer_state` | VARCHAR(2) | Estado de Brasil (sigla, ej. SP) |
| `customer_city` | VARCHAR | Ciudad del cliente |
| `precio_total` | NUMERIC | Suma de precios de ítems de la orden |
| `flete_total` | NUMERIC | Suma de flete de ítems de la orden |
| `ratio_flete` | NUMERIC | `flete_total / precio_total` |
| `review_score` | NUMERIC | Promedio de reviews de la orden (1–5) |
| `categoria_producto` | VARCHAR | Categoría del primer ítem de la orden |

### `kpis_por_mes` — KPIs generales

| Columna | Descripción |
|---------|-------------|
| `mes` | Mes (PK) |
| `gmv_total` | Gross Merchandise Value |
| `ticket_promedio` | Ticket medio |
| `ticket_mediana` | Mediana del ticket |
| `pct_entregas_a_tiempo` | % de órdenes entregadas en tiempo |
| `score_promedio_reviews` | Promedio de review score |
| `tasa_retencion` | % de clientes que compraron en meses anteriores |
| `ratio_flete_gmv` | Flete como % del GMV |
| `cantidad_ordenes` | Cantidad de órdenes del mes |

### `metricas_por_categoria` — por categoría de producto

| Columna | Descripción |
|---------|-------------|
| `mes`, `categoria` | PK compuesta |
| `gmv_total` | GMV de la categoría en el mes |
| `cantidad_ordenes` | Órdenes de la categoría |
| `ticket_promedio` | Ticket medio de la categoría |

### `metricas_por_estado` — por estado geográfico

| Columna | Descripción |
|---------|-------------|
| `mes`, `estado` | PK compuesta (estado = sigla de 2 letras) |
| `gmv_total` | GMV del estado en el mes |
| `cantidad_ordenes` | Órdenes del estado |
| `pct_entregas_a_tiempo` | % entregas a tiempo en el estado |
| `score_promedio_reviews` | Score promedio del estado |

### `estados_traduccion` y `categorias_traduccion` — tablas de referencia para el dashboard

El dashboard muestra los estados y categorías con su **nombre completo**, no con siglas ni códigos. Estas dos tablas de traducción son las que Metabase usa para los filtros y selecciones:

- `estados_traduccion`: mapea la sigla de 2 letras (`SP`, `RJ`, `MG`…) al nombre completo del estado brasileño (`São Paulo`, `Río de Janeiro`, `Minas Gerais`…).
- `categorias_traduccion`: mapea el nombre de categoría en inglés (`health_beauty`, `sports_leisure`…) a su nombre en español para la visualización (`Salud y Belleza`, `Deportes y Ocio`…).

Ambas tablas se crean y populan automáticamente al levantar el stack (`init_dwh.sql`).

---

## Metabase

1. Abrir http://localhost:3000
2. Iniciar sesión con `admin@blackfriday.local` / `BlackFridayLab1`
3. Añadir base de datos:
   - Tipo: PostgreSQL
   - Host: `postgres-dwh`
   - Puerto: `5432`
   - Base de datos: `dwh`
   - Usuario: `dwh`
   - Contraseña: `dwh123`
4. Construir dashboards sobre `kpis_por_mes`, `metricas_por_categoria` y `metricas_por_estado`.

> Los filtros del dashboard usan los nombres completos de estados y categorías, no siglas ni códigos. Las tablas `estados_traduccion` y `categorias_traduccion` del DWH proveen esos nombres para Metabase.

---

## Estructura del proyecto

```
olist_project/
├── dags/
│   ├── olist_pipeline.py          # DAG principal del proyecto
│   └── black_friday_pipeline.py   # DAG de ejemplo heredado (no se usa)
├── data/
│   ├── input/                     # Colocar los CSVs de Olist aquí
│   └── output/                    # Salidas auxiliares opcionales
├── api/                           # Servicio heredado del template base
│   ├── app.py                     # (no se usa en el pipeline de Olist)
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   └── metabase_auto_setup.py     # Crea el usuario admin de Metabase
├── docker-compose.yml
├── Dockerfile.airflow
├── init_dwh.sql                   # Schema del DWH (tablas de Olist)
├── init_source.sql                # Schema heredado (no se usa)
├── init_db.sql                    # Referencia
├── requirements.txt
├── GUIA_DOCKERFILE.md
└── README.md
```

> La carpeta `api/` y el servicio `postgres-source` se mantienen en `docker-compose.yml` porque su remoción rompe la construcción del stack. No son utilizados por el pipeline de Olist.

---

## Decisiones técnicas

**Batch idempotente sobre streaming.** El dataset de Olist es histórico (no hay eventos en tiempo real), por lo que micro-batch o streaming no agregan valor. El DAG puede re-ejecutarse sin duplicar datos gracias al UPSERT por `order_id`.

**Transformaciones en pandas + agregaciones en SQL.** La limpieza y el join de múltiples CSVs se hace en pandas (más legible). Las agregaciones se calculan directamente en PostgreSQL con SQL para evitar traer cientos de miles de filas a memoria de Airflow innecesariamente.

**Validaciones tempranas.** `discover_inputs` y `validate_schema` fallan antes de leer o transformar datos, lo que permite detectar CSVs faltantes o corruptos sin consumir recursos.

**Traducción de categorías.** Si `product_category_name_translation.csv` está presente, el pipeline reemplaza los nombres de categoría en portugués por sus equivalentes en inglés. El DWH también incluye la tabla `categorias_traduccion` para filtros en Metabase.

---

## Comandos útiles

```bash
# Levantar todo o reconstruir si cambió la imagen de Airflow
docker compose up -d --build

# Ver logs del scheduler
docker compose logs -f airflow-scheduler

# Ver logs de una ejecución específica del DAG (desde Airflow UI)
# Airflow UI → DAGs → olist_pipeline → Graph → clic en tarea → Logs

# Bajar el stack
docker compose down

# Bajar y limpiar volúmenes (borra datos del DWH y Metabase)
docker compose down -v
```

### Rutas dentro del contenedor de Airflow

| Uso | Ruta en tu máquina | Ruta dentro de Airflow |
|-----|-------------------|------------------------|
| DAGs | `./dags` | `/opt/airflow/dags` |
| Datos de entrada | `./data/input` | `/opt/airflow/data/input` |
| Salidas auxiliares | `./data/output` | `/opt/airflow/data/output` |
| Logs de Airflow | `./logs` | `/opt/airflow/logs` |

