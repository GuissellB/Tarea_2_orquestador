# Tarea 2 - ETL de Clima con Prefect, MongoDB y Git

Este proyecto implementa un flujo **ETL** que consulta el clima actual desde OpenWeather, transforma la respuesta, guarda un JSON intermedio y carga el resultado en MongoDB.

## 1. Que se hizo

Se desarrollo un pipeline en `practica.py` con Prefect:

- **Extract** (`extraer_datos_clima`): consulta OpenWeather para una ciudad.
- **Transform** (`transformar_datos`): valida campos y normaliza la estructura final.
- **Load**:
  - `guardar_json`: escribe `clima_data.json` como respaldo intermedio.
  - `leer_json`: valida la lectura del JSON.
  - `cargar_a_mongo`: inserta el documento en MongoDB Atlas.

Ademas:

- Se agregaron **retries** en tareas criticas (API, JSON, MongoDB).
- Se incorporo **logging estructurado** con Prefect.
- El flujo principal `clima_flow` orquesta todo y propaga errores para que Prefect marque estados correctamente.

## 2. Estructura principal

- `practica.py`: flujo ETL y tareas.
- `prefect.yaml`: definicion del deployment para Prefect Cloud.
- `requirements.txt`: dependencias.
- `.env`: variables de entorno locales (no versionado).
- `.gitignore` y `.prefectignore`: exclusion de secretos/artefactos.

## 3. Requisitos

- Python 3.10+
- Cuenta en OpenWeather (API key)
- MongoDB Atlas (URI de conexion)
- Cuenta/Workspace en Prefect Cloud
- Git

## 4. Configuracion local

Crear y activar entorno virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Instalar dependencias:

```powershell
pip install -r requirements.txt
```

Crear archivo `.env` en la raiz (ejemplo):

```env
MONGO_URI=mongodb+srv://<usuario>:<password>@<cluster>.mongodb.net/
OPENWEATHER_API_KEY=<tu_api_key>
MONGO_DB_NAME=clima_data
MONGO_COLLECTION_NAME=clima_data
CITY=San Jose,CR
```

## 5. Ejecucion local del flujo

Desde la raiz del proyecto:

```powershell
python practica.py
```

Resultado esperado:

- Se genera/actualiza `clima_data.json`.
- Se inserta un documento en la coleccion configurada en MongoDB.

## 6. Ejecucion en Prefect Cloud

El proyecto ya incluye deployment definido en `prefect.yaml`:

- Deployment: `clima-1h`
- Entrypoint: `practica.py:clima_flow`
- Work pool: `managed-pool`
- Schedule: cada 3600 segundos (1 hora), zona horaria `America/Costa_Rica`
- `pull` step: clona `main` desde `https://github.com/GuissellB/Tarea_2_orquestador.git`

### 6.1 Login y configuracion de Prefect Cloud

```powershell
python -m prefect cloud login
```

Seleccionar workspace cuando lo solicite.

### 6.2 Crear/validar work pool

En Prefect Cloud, crear un work pool llamado `managed-pool` (o actualizar `prefect.yaml` con el nombre real del pool que uses).

### 6.3 Publicar deployment

```powershell
python -m prefect deploy -n clima-1h
```

Esto registra el deployment en Cloud usando la configuracion del `prefect.yaml`.

### 6.4 Ejecutar manualmente un run (opcional)

```powershell
python -m prefect deployment run "etl-clima-openweather-mongo/clima-1h"
```

Tambien puedes ejecutarlo desde la UI de Prefect Cloud.

## 7. Flujo con Git (recomendado)

El deployment usa `git_clone`, por lo que **Prefect ejecuta siempre el codigo del repositorio remoto**.

Ciclo recomendado:

1. Hacer cambios localmente.
2. Probar local (`python practica.py`).
3. Commit y push a `main`:

```powershell
git add .
git commit -m "feat: ajustes ETL clima"
git push origin main
```

4. Si cambiaste `prefect.yaml` o definicion del deployment, volver a publicar:

```powershell
python -m prefect deploy -n clima-1h
```

## 8. Evidencias de ejecucion

Se creo una carpeta de evidencias con imagenes de las ejecuciones realizadas (En Prefect Cloud) para documentar el funcionamiento del flujo.
- `evidencias/prefect_deployment.png`
- `evidencias/prefect_run_exitoso.png`
## 9. Seguridad y buenas practicas

- No subir `.env` al repositorio.
- Rotar credenciales si alguna fue expuesta.
- Mantener `.gitignore` y `.prefectignore` actualizados.
- Para produccion, administrar secretos con Prefect (Variables/Blocks) en lugar de archivo `.env`.

## 10. Verificacion rapida

Checklist:

- Dependencias instaladas
- `.env` configurado
- Conexion a MongoDB valida
- API key de OpenWeather activa
- Deployment `clima-1h` en Prefect Cloud
- Work pool `managed-pool` disponible y con workers/infra activos


