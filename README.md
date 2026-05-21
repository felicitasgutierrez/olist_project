# Black Friday Pipeline - Proyecto de Ingeniería de Datos

Pipeline orquestado con Apache Airflow para monitorear ventas y promociones en tiempo real durante Black Friday, integrando tiendas físicas (PostgreSQL) y app móvil (API FastAPI).

El objetivo es usarlo como **laboratorio base de arquitectura**: Docker levanta Airflow, PostgreSQL y Metabase; el DAG Black Friday queda como ejemplo de referencia, pero los alumnos deberían crear **su propio DAG** para el dataset asignado.

Para enlazar el **ejercicio de los 8 pasos** del material de clase con el código y Airflow, seguí la sección **Guía de práctica** más abajo. Para abrir Airflow, Metabase y la API con instrucciones detalladas, ver **[PASO_A_PASO.md](PASO_A_PASO.md)**. Para modificar librerías Python en la imagen de Airflow, ver **[GUIA_DOCKERFILE.md](GUIA_DOCKERFILE.md)**.

## Inicio rápido

```bash
docker compose up -d --build
```

- **Airflow UI**: http://localhost:8080 (usuario: `admin`, contraseña: `admin`)
- **API ventas app**: http://localhost:8000/sales
- **Metabase**: http://localhost:3000 — login: `admin@blackfriday.local` / `BlackFridayLab1` (lo crea el servicio `metabase-setup` la primera vez; ver `PASO_A_PASO.md` si ya tenías un volumen viejo)

---

## Uso recomendado para la materia

Sí, la propuesta es viable: Airflow es una buena incorporación para que practiquen **orquestación**, dependencias, scheduling, retries, logs y monitoreo sin sumar todavía una plataforma distribuida pesada. La aclaración importante es pedagógica: **Airflow no reemplaza a pandas/polars ni a la base de datos**; coordina cuándo corre cada etapa y qué pasa si falla.

La consigna sugerida para los alumnos:

1. Crear un DAG nuevo en `dags/` para su dataset.
2. Leer archivos de entrada desde `/opt/airflow/data/input` dentro del contenedor de Airflow. En la máquina host esa ruta corresponde a `./data/input`.
3. Validar que todos los archivos respeten el mismo contrato de columnas.
4. Transformar y limpiar con pandas o polars.
5. Si alguna métrica requiere datos externos, llamar a la API desde una tarea separada con timeout, retries y manejo de errores.
6. Cargar datos limpios o intermedios en PostgreSQL DWH (`postgres-dwh`).
7. Calcular y persistir agregaciones ya listas para el dashboard.
8. Construir el dashboard en Metabase sobre las tablas agregadas, no sobre archivos crudos.

### Convención de carpetas y rutas

| Uso | Ruta en tu máquina | Ruta dentro de Airflow |
|-----|--------------------|------------------------|
| DAGs | `./dags` | `/opt/airflow/dags` |
| Datos de entrada | `./data/input` | `/opt/airflow/data/input` |
| Salidas auxiliares opcionales | `./data/output` | `/opt/airflow/data/output` |
| Logs de Airflow | `./logs` | `/opt/airflow/logs` |

Si la carpeta `data/input` no existe, crearla antes de copiar datasets:

```bash
mkdir -p data/input data/output
```

### Contrato mínimo del DAG que deberían entregar

El DAG debería verse conceptualmente así:

```
discover_inputs
  >> validate_schema
  >> extract_or_read
  >> clean_transform
  >> optional_external_api_enrichment
  >> load_detail_or_staging
  >> build_aggregations
  >> quality_checks
```

Condiciones esperadas:

- **Generalizable**: no hardcodear un único archivo; procesar todos los archivos compatibles de `data/input`.
- **Idempotente**: si el DAG se reejecuta, no debe duplicar métricas ni romper tablas. Usar claves naturales, `UPSERT` o estrategia de borrado/recarga por partición.
- **Observable**: cada tarea debe dejar logs claros y fallar si el input no cumple el contrato.
- **Separación de responsabilidades**: una tarea lee, otra valida, otra transforma, otra carga, otra calcula agregados.
- **Capa BI limpia**: Metabase debería consultar tablas agregadas del DWH, por ejemplo `ventas_por_dia`, `metricas_por_categoria`, `ranking_productos`, etc.
- **Dependencias reproducibles**: si necesitan una librería Python nueva, agregarla al `Dockerfile.airflow` y reconstruir la imagen. Ver `GUIA_DOCKERFILE.md`.

### Servicios disponibles para sus DAGs

- **Airflow**: orquestación y monitoreo del pipeline.
- **PostgreSQL DWH (`postgres-dwh`)**: base destino para staging, detalle limpio y agregaciones.
- **Metabase**: dashboard final conectado al DWH.
- **API de ejemplo (`api-sales`)**: queda como muestra de integración HTTP; para la entrega pueden consumir otra API si la consigna lo requiere.
- **PostgreSQL source (`postgres-source`)**: queda como fuente transaccional de ejemplo; no es obligatorio usarla si el trabajo parte de archivos.

Conexiones ya configuradas en Airflow por variables de entorno:

- `postgres_dwh`: `postgres://dwh:dwh123@postgres-dwh:5432/dwh`
- `postgres_source`: `postgres://stores:stores123@postgres-source:5432/stores_sales`

---

## Guía de práctica (material: los 8 pasos del pipeline)

Esta sección describe el **ejemplo Black Friday incluido**. Sirve para mostrar el patrón, no para limitar la entrega de los alumnos.

Esta práctica implementa el caso del material de clase: **monitorear ventas y promociones en Black Friday** con un **único dashboard** que una **tiendas físicas** y **app**. En el PDF se pide reflexionar cada paso del diseño; aquí ves **cómo queda reflejado en el código y en Airflow**.

| Paso (diseño) | En este proyecto |
|---------------|------------------|
| 1. Objetivo | Métricas por minuto en `sales_realtime_metrics` para ventas y % promos, canal físico vs app |
| 2. Fuentes | `postgres-source` + `api-sales` |
| 3. Ingestión | Micro-batch cada 1 min (DAG), CDC por `last_updated`, GET `/sales` |
| 4. Procesamiento | `transform_unify` — normalización, unificación, descarte montos &lt; 0 |
| 5. Almacenamiento salida | DWH PostgreSQL: `stg_sales_unified` + `sales_realtime_metrics` |
| 6. Flujo de datos | Grafo de tareas del DAG (dependencias explícitas) |
| 7. Gobernanza / monitoreo | Retries, validación de conexiones, `quality_check`, logs |
| 8. Consumo | Metabase sobre `postgres-dwh` |

### 1) Objetivo del pipeline

**Qué definir en el diseño:** usuarios finales (ventas, gerencia), preguntas de negocio (¿cómo van las ventas y las promos?, ¿físico vs app?), producto final (dashboard), métricas de éxito del pipeline.

**Cómo está en el script:** el producto analítico es la tabla agregada `sales_realtime_metrics` (totales, conteo, ticket medio, % uso de promoción, desglose físico/app por **minuto**). Eso responde al “tiempo casi real” del enunciado sin ser streaming puro. Detalle de columnas más abajo en este README.

### 2) Selección de fuentes de datos

**Qué definir en el diseño:** qué sistemas alimentan el dashboard, formato, volumen, velocidad de cambio, si hay historia que preservar.

**Cómo está en el proyecto:**

- **Tiendas físicas:** PostgreSQL en el servicio `postgres-source`, base `stores_sales`, tabla `sales_stores` (ver `init_source.sql`). Incluye `last_updated` para identificar **qué filas cambiaron** en cada corrida.
- **App móvil:** API FastAPI en `api/app.py`, expuesta como servicio `api-sales`; el DAG consume `http://api-sales:8000/sales` (desde dentro de Docker; desde tu máquina, puerto 8000).

### 3) Estrategia de ingesta

**Qué definir en el diseño:** batch vs streaming, herramienta de orquestación, staging intermedio si hace falta.

**Cómo está en el script:**

- **Micro-batch:** `schedule="*/1 * * * *"` en `dags/black_friday_pipeline.py` — Airflow dispara el DAG cada minuto.
- **CDC en tiendas:** `_extract_stores` lee solo filas con `last_updated` en la ventana del intervalo de Airflow (más un buffer de 1 minuto para no perder inserciones del `seed`).
- **API:** `_extract_app` hace `requests.get` con timeout; filtra registros por ventana de tiempo respecto a `sale_timestamp`.
- **Simulación de carga:** `_seed_new_sales` inserta ventas de prueba en origen para que cada minuto haya datos nuevos en laboratorio.

### 4) Plan de procesamiento (transformaciones)

**Qué definir en el diseño:** limpieza, reglas de calidad, qué columnas se usan, manejo de nulos/erróneos, duplicados.

**Cómo está en el script:**

- **`_normalize_row`:** unifica esquema (ids, timestamp, monto, canal, promo, `source_system`).
- **`_transform_unify`:** junta salidas de `extract_stores` y `extract_app` (vía XCom), aplica control de calidad **descartando montos negativos** y registra advertencias en log.
- Los datos listos para cargar pasan serializados en XCom al siguiente paso (timestamps en ISO).

### 5) Arquitectura de almacenamiento del resultado

**Qué definir en el diseño:** DWH vs lake, tablas finales, relaciones, dónde vive el detalle vs el agregado.

**Cómo está en el proyecto:**

- **`init_dwh.sql`:** define el esquema del DWH en `postgres-dwh`.
- **`stg_sales_unified`:** staging con el detalle unificado por transacción (incluye `batch_minute`).
- **`sales_realtime_metrics`:** capa agregada por minuto para alimentar el dashboard (UPSERT por `metric_minute` en `_load_and_aggregate`).

### 6) Planificación del flujo de datos

**Qué definir en el diseño:** orden de jobs, dependencias, paralelismo, qué pasa si falla un paso.

**Cómo está en el script (grafo del DAG):**

```
[validate_source, validate_dwh] >> seed_sales >> [extract_stores, extract_app]
  >> transform_unify >> load_aggregate >> quality_check
```

- Validaciones y `seed` van **antes** de los extracts.
- **Paralelo:** `extract_stores` y `extract_app` tras `seed_sales`.
- **Secuencial:** transform → carga → chequeo final.
- **Reintentos:** `default_args` con `retries: 3` y backoff exponencial.

### 7) Gobernanza y monitoreo

**Qué definir en el diseño:** qué vigilar (fallos, calidad, recursos), políticas de acceso, alertas.

**Cómo está en el proyecto:**

- **Conectividad:** `validate_source` y `validate_dwh` (`PostgresOperator`, `SELECT 1`).
- **Calidad post-carga:** `quality_check` cuenta filas con monto negativo en staging y en métricas; si hay, **falla la tarea** para que Airflow lo marque.
- **Trazas:** logging en operadores Python; historial de ejecuciones y estados en la UI de Airflow (`http://localhost:8080`).
- *Nota de laboratorio:* credenciales van en variables de entorno de `docker-compose.yml`; en producción conviene secretos gestionados (Vault, etc.), como sugiere el marco teórico.

### 8) Capa de consumo

**Qué definir en el diseño:** herramienta de BI, quién accede, refresco de datos, KPIs concretos del dashboard.

**Cómo está en el proyecto:** **Metabase** se conecta a `postgres-dwh` (host `postgres-dwh` desde otro contenedor en la misma red). Ahí se construyen preguntas y dashboards sobre `sales_realtime_metrics` y, si hace falta drill-down, `stg_sales_unified`. Pasos de conexión en la sección **Metabase** más abajo.

### Qué hacer en la práctica (checklist)

1. Levantar el stack: `docker-compose up -d`.
2. En Airflow, activar el DAG `black_friday_pipeline` y observar ejecuciones minuto a minuto.
3. Revisar en el grafo cómo cada bloque del PDF (pasos 1–8) aparece como **tarea o tabla**.
4. Opcional: en Metabase, crear un gráfico de `total_sales` o `promo_usage_percent` vs `metric_minute`.
5. Para la entrega del **ejercicio teórico del PDF**, redactar **tu** versión de los 8 pasos (arquitectura, riesgos, alternativas); este repo es una **implementación posible**, no la única respuesta válida.

---

## Arquitectura

```
┌─────────────────┐     ┌─────────────────┐
│ postgres-source │     │   api-sales     │
│ (tiendas fís.)  │     │   (FastAPI)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │    CDC last_updated   │   GET /sales
         │    Extract cada 1min  │
         ▼                       ▼
┌─────────────────────────────────────────┐
│           Airflow (scheduler)           │
│  black_friday_pipeline DAG              │
│  - Extract (PostgreSQL + API)           │
│  - Transform (normalizar, validar)      │
│  - Load (staging → sales_realtime_metrics)│
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│           postgres-dwh                  │
│  - stg_sales_unified                    │
│  - sales_realtime_metrics               │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│              Metabase                   │
│         (dashboards y reportes)         │
└─────────────────────────────────────────┘
```

---

## Decisiones técnicas

### Fuentes de datos

- **PostgreSQL (postgres-source)**: Simula ventas de tiendas físicas con tabla `sales_stores` y columna `last_updated` para CDC.
- **API FastAPI**: Simula ventas de la app móvil. Retorna JSON con ventas aleatorias en cada request.

### CDC (Change Data Capture)

- Se usa la columna `last_updated` en la fuente de tiendas.
- En cada ejecución se extraen solo las filas con `last_updated` en la ventana del batch (intervalo + buffer de 1 min).
- Evita reprocesar datos ya cargados.

### Micro-batch vs streaming

- **Por qué micro-batch (cada 1 minuto)**:
  - Orquestación clara con Airflow (schedules, retries, monitoreo).
  - Menor complejidad que Kafka/Flink.
  - Suficiente para métricas “casi tiempo real” en Black Friday.
  - Mejor manejo de fallos y backpressure.
- **Cuándo streaming tendría sentido**: Si se necesitara latencia &lt; 1 segundo o procesamiento evento a evento (alerts, recomendaciones en vivo).

### Control de calidad

- Validación de montos no negativos en la transformación.
- Tarea `quality_check` que verifica integridad en el DWH.
- Descartado de registros inválidos con logging.

### Resiliencia

- `retries: 3` con backoff exponencial.
- Validación de conexiones (PostgresOperator) al inicio del DAG.
- Timeouts en llamadas HTTP a la API.

---

## Estructura del proyecto

```
black_friday_project/
├── dags/
│   └── black_friday_pipeline.py   # DAG de ejemplo; los alumnos agregan su propio DAG aquí
├── data/
│   ├── input/                     # Datasets de entrada para DAGs de estudiantes
│   └── output/                    # Salidas auxiliares opcionales
├── api/
│   ├── app.py                     # FastAPI ventas app
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── Dockerfile.airflow
├── GUIA_DOCKERFILE.md            # Cómo agregar librerías Python a la imagen de Airflow
├── init_source.sql                # Schema y datos iniciales tiendas
├── init_dwh.sql                   # Schema DWH
├── init_db.sql                    # Referencia
├── scripts/
│   └── metabase_auto_setup.py     # Crea admin de Metabase (servicio metabase-setup)
├── requirements.txt
├── PASO_A_PASO.md
└── README.md
```

---

## Tabla agregada: `sales_realtime_metrics`

| Columna              | Tipo      | Descripción                          |
|----------------------|-----------|--------------------------------------|
| metric_minute        | TIMESTAMP | Minuto de la métrica                 |
| total_sales          | DECIMAL   | Total de ventas                      |
| sales_count          | INTEGER   | Cantidad de transacciones            |
| sales_per_minute     | DECIMAL   | Ventas en ese minuto                 |
| sales_physical_store | DECIMAL   | Ventas tiendas físicas               |
| sales_app            | DECIMAL   | Ventas app móvil                     |
| promo_usage_percent  | DECIMAL   | % de ventas con promoción            |
| avg_ticket           | DECIMAL   | Ticket promedio                      |

---

## Metabase

1. Abrir http://localhost:3000.
2. Iniciar sesión con **admin@blackfriday.local** / **BlackFridayLab1** (las crea el contenedor `metabase-setup` en un arranque nuevo; si ya tenías datos viejos en el volumen de Metabase, ver `PASO_A_PASO.md`).
3. Añadir base de datos:
   - Tipo: PostgreSQL
   - Host: `postgres-dwh`
   - Puerto: 5432
   - Base: `dwh`
   - Usuario: `dwh`
   - Contraseña: `dwh123`
4. Crear dashboards sobre `sales_realtime_metrics` y `stg_sales_unified`.

---

## Escalabilidad

- **Más archivos del mismo input**: descubrir archivos en `data/input`, validar esquema y procesarlos en loop o con task mapping.
- **Más fuentes**: añadir tareas de extract y ramas de transform para cada fuente.
- **Mayor volumen**: mantener Airflow como orquestador y mover el procesamiento pesado a la base, Spark, dbt u otro motor; luego, si hace falta, pasar Airflow a CeleryExecutor/KubernetesExecutor.
- **Menor latencia**: reducir el schedule con cuidado de carga; para el laboratorio, minutos u horas suele ser suficiente.
- **Streaming real**: Sustituir micro-batch por Kafka + Kafka Connect + Flink/Spark.
- **Particionamiento**: partir tablas agregadas por fecha, período o dataset.
- **Caching**: Redis para métricas calientes si las consultas se disparan.

---

## Comandos útiles

```bash
# Levantar todo o reconstruir si cambió la imagen de Airflow
docker compose up -d --build

# Ver logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Bajar
docker compose down
```

---

## DAG: `black_friday_pipeline`

- **Schedule**: `*/1 * * * *` (cada minuto)
- **Tareas**:
  1. `validate_source` (PostgresOperator) – Verifica conexión a postgres-source
  2. `validate_dwh` (PostgresOperator) – Verifica conexión a postgres-dwh
  3. `seed_sales` (PythonOperator) – Inserta ventas simuladas en tiendas
  4. `extract_stores` (PythonOperator) – Extrae con CDC por `last_updated`
  5. `extract_app` (PythonOperator) – Llama a la API y filtra por intervalo
  6. `transform_unify` (PythonOperator) – Normaliza y aplica reglas de calidad
  7. `load_aggregate` (PythonOperator) – Inserta en DWH y actualiza `sales_realtime_metrics`
  8. `quality_check` (PythonOperator) – Valida ausencia de montos negativos
