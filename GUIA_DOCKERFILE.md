# Guia para modificar el Dockerfile de Airflow

Esta guia explica como pensar el archivo `Dockerfile.airflow` cuando necesitan agregar o cambiar librerias de Python para sus DAGs.

La idea central es cambiar el mindset:

- Antes, en proyectos locales, usaban un `venv`.
- En este laboratorio, el entorno de Python vive dentro de una **imagen Docker**.
- Esa imagen es la "maquina Python reproducible" que Airflow usa para ejecutar los DAGs.

En vez de decir:

```bash
python -m venv .venv
pip install pandas requests
```

aca la idea es declarar las dependencias en el `Dockerfile.airflow` y reconstruir la imagen:

```bash
docker compose up -d --build
```

---

## Por que no usar venv dentro de este proyecto

Un `venv` sirve muy bien cuando ejecutamos Python directo en nuestra computadora. Pero Airflow, en este laboratorio, corre dentro de contenedores Docker.

Eso significa que:

- Si instalan una libreria en el `venv` de su maquina, Airflow no la ve.
- Airflow ejecuta el DAG usando el Python instalado dentro de la imagen Docker.
- Para que todos tengan el mismo entorno, las librerias deben quedar declaradas en la imagen.

La ventaja es fuerte para trabajos grupales y entregas:

- El docente puede levantar el proyecto sin adivinar que instalaron.
- Todos los integrantes usan el mismo Python y las mismas versiones.
- El entorno se puede borrar y reconstruir.
- El pipeline deja de depender de "en mi compu funciona".

---

## Que archivo se modifica

El archivo a modificar es:

```text
Dockerfile.airflow
```

Ese archivo define la imagen usada por:

- `airflow-scheduler`
- `airflow-webserver`
- `airflow-init`

Es decir: si un DAG necesita una libreria Python, esa libreria debe estar disponible en la imagen creada por `Dockerfile.airflow`.

---

## Como leer el Dockerfile actual

Contenido actual simplificado:

```dockerfile
FROM apache/airflow:2.7.3-python3.11

ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    apache-airflow-providers-postgres==5.8.0 \
    requests==2.31.0 \
    python-dateutil==2.8.2 \
    "pandas>=2.0,<3" \
    "polars>=0.20,<2" \
    "pyarrow>=14,<22" \
    "openpyxl>=3.1,<4"
```

### `FROM apache/airflow:2.7.3-python3.11`

Indica la imagen base.

No partimos de `python:3.11`, porque necesitamos Airflow ya instalado y configurado con una estructura compatible.

Esta linea dice:

- Airflow version `2.7.3`
- Python version `3.11`

### `ARG AIRFLOW_CONSTRAINTS_URL=...`

Airflow recomienda instalar librerias usando un archivo de constraints compatible con la version de Airflow.

Ese archivo ayuda a evitar combinaciones rotas de dependencias.

Sin constraints, puede pasar que `pip install` actualice una libreria interna y Airflow empiece a fallar.

### `USER root`

Cambia al usuario administrador del contenedor.

Se usa solo cuando necesitamos instalar paquetes del sistema operativo, por ejemplo:

- `curl`
- `gcc`
- `libpq-dev`
- librerias nativas requeridas por algun paquete Python

### `RUN apt-get ...`

Instala dependencias del sistema operativo dentro de la imagen.

No es lo mismo que `pip install`.

- `apt-get` instala programas o librerias de Linux.
- `pip install` instala paquetes Python.

### `USER airflow`

Vuelve al usuario normal recomendado para ejecutar Airflow.

La regla practica es:

- Usar `root` solo para instalaciones de sistema.
- Volver a `airflow` antes de instalar paquetes Python y ejecutar Airflow.

### `RUN pip install ...`

Instala las librerias Python disponibles para los DAGs.

Por ejemplo, gracias a esta parte los DAGs pueden hacer:

```python
import pandas as pd
import polars as pl
import requests
```

---

## Como agregar una libreria Python

Supongamos que un grupo necesita `sqlalchemy` o `scikit-learn`.

Se agrega al final del bloque `pip install`:

```dockerfile
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    apache-airflow-providers-postgres==5.8.0 \
    requests==2.31.0 \
    python-dateutil==2.8.2 \
    "pandas>=2.0,<3" \
    "polars>=0.20,<2" \
    "pyarrow>=14,<22" \
    "openpyxl>=3.1,<4" \
    "scikit-learn>=1.4,<2"
```

Detalles importantes:

- Cada linea termina con `\`, excepto la ultima.
- Conviene fijar rangos de versiones, no dejar todo sin version.
- Despues de modificar el Dockerfile, hay que reconstruir la imagen.

Comando:

```bash
docker compose up -d --build
```

Si los contenedores ya estaban corriendo, Compose reconstruye la imagen y recrea los servicios que la usan.

---

## Como agregar una dependencia del sistema

Algunas librerias Python necesitan paquetes del sistema operativo.

Ejemplo: si una libreria necesita compilar codigo nativo, puede hacer falta `gcc`.

Se agrega en el bloque de `apt-get`:

```dockerfile
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
```

Despues, como siempre:

```bash
docker compose up -d --build
```

Regla practica:

- Si el error dice `No module named ...`, probablemente falta un paquete Python en `pip install`.
- Si el error dice `gcc not found`, `pg_config executable not found`, `cannot find -l...` o similar, probablemente falta una dependencia del sistema con `apt-get`.

---

## Como verificar que una libreria quedo instalada

Con los servicios levantados, pueden entrar al contenedor del scheduler:

```bash
docker compose exec airflow-scheduler python -c "import pandas; print(pandas.__version__)"
```

Otro ejemplo:

```bash
docker compose exec airflow-scheduler python -c "import polars; print(polars.__version__)"
```

Si el comando funciona, Airflow tambien deberia poder importar esa libreria desde un DAG.

---

## Dockerfile vs requirements.txt

En este proyecto hay tambien un `requirements.txt`.

Para este laboratorio, el archivo principal para Airflow es:

```text
Dockerfile.airflow
```

El `requirements.txt` queda como referencia para desarrollo local opcional, pero no alcanza para modificar el Python que usa Airflow dentro de Docker.

Si agregan una libreria que el DAG necesita, lo importante es agregarla al `Dockerfile.airflow`.

---

## Ciclo de trabajo recomendado

1. Escribir o modificar el DAG en `dags/`.
2. Si falta una libreria, agregarla en `Dockerfile.airflow`.
3. Reconstruir:

   ```bash
   docker compose up -d --build
   ```

4. Verificar que Airflow levanto:

   ```bash
   docker compose ps
   ```

5. Mirar logs del scheduler si el DAG no aparece o aparece con error:

   ```bash
   docker compose logs -f airflow-scheduler
   ```

6. Abrir Airflow en http://localhost:8080 y revisar el DAG.

---

## Errores comunes

### Instale con pip en mi maquina, pero Airflow no lo encuentra

Eso ocurre porque instalaron en el Python local, no en la imagen Docker.

Solucion:

1. Agregar la libreria al `Dockerfile.airflow`.
2. Ejecutar `docker compose up -d --build`.

### Modifique el Dockerfile, pero sigue sin aparecer la libreria

Puede que no se haya reconstruido la imagen.

Probar:

```bash
docker compose build --no-cache airflow-init airflow-scheduler airflow-webserver
docker compose up -d
```

### El DAG aparece roto en Airflow

Revisar logs:

```bash
docker compose logs -f airflow-scheduler
```

Tambien pueden probar el import dentro del contenedor:

```bash
docker compose exec airflow-scheduler python -c "import nombre_libreria"
```

### La instalacion tarda mucho

Es normal la primera vez. Docker guarda capas en cache, asi que las siguientes reconstrucciones suelen ser mas rapidas.

---

## Regla mental final

Para este laboratorio:

```text
venv local = entorno para mi maquina
Dockerfile = entorno reproducible para el pipeline
imagen Docker = Python + librerias + Airflow listos para ejecutar DAGs
docker compose = forma de levantar todos los servicios juntos
```

Si el pipeline depende de una libreria, esa dependencia debe estar escrita en la imagen. Esa es la diferencia entre un script local y un servicio reproducible.
