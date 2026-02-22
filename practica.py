from pymongo import MongoClient
import requests
from prefect import flow, task, get_run_logger
from datetime import datetime
import json
import os
from pathlib import Path
from time import perf_counter
from typing import Any, Dict
import unicodedata 

def load_env_file(path: Path | None = None) -> None:
    """
    Carga variables de entorno desde .env sin dependencias externas.
    No sobrescribe variables ya definidas en el sistema.
    """
    # Robusto para deployments: resuelve .env relativo a este archivo.
    env_path = path or (Path(__file__).resolve().parent / ".env")
    if not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Variable de entorno requerida no definida: {var_name}")
    return value

load_env_file()

MONGO_URI = get_required_env("MONGO_URI")
API_KEY = get_required_env("OPENWEATHER_API_KEY")
DB_NAME = os.getenv("MONGO_DB_NAME", "clima_data")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "clima_data")
DEFAULT_CITY = os.getenv("CITY", "San Jose,CR")

@task(
    name="extraer_datos_clima",
    retries=3,  #Retry automático para fallos temporales de red/API
    retry_delay_seconds=5,
)
def extraer_datos_clima(ciudad: str) -> Dict[str, Any]:
    logger = get_run_logger()
    start = perf_counter()
    url = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"

    try:
        logger.info("event=extract_start ciudad=%s", ciudad)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Validación temprana: la API debe devolver un objeto con contenido.
        if not isinstance(data, dict) or not data:
            raise ValueError("Respuesta vacía o inválida de la API.")

        logger.info(
            "event=extract_success ciudad=%s status_code=%s",
            ciudad,
            response.status_code,
        )
        return data
    except Exception:
        # Re-lanzamos para que Prefect registre estado FAILED o RETRY.
        logger.exception("event=extract_error ciudad=%s", ciudad)
        raise
    finally:
        logger.info("event=extract_time ciudad=%s seconds=%.3f", ciudad, perf_counter() - start)


@task(name="transformar_datos")
def transformar_datos(data: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    start = perf_counter()

    try:
        logger.info("event=transform_start")

        # Validaciones mínimas de estructura requerida para transformar.
        required_top_keys = ["name", "sys", "main", "weather", "clouds", "coord"]
        missing = [k for k in required_top_keys if k not in data]
        if missing:
            raise ValueError(f"Faltan claves requeridas en la respuesta: {missing}")
        if not data.get("weather"):
            raise ValueError("La clave 'weather' no contiene información.")

        registro_clima = {
            "ciudad": data["name"],
            "pais": data["sys"]["country"],
            "temperatura": data["main"]["temp"],
            "sensacion_termica": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "humedad": data["main"]["humidity"],
            "presion": data["main"]["pressure"],
            "descripcion": data["weather"][0]["description"],
            "icono": data["weather"][0]["icon"],
            "nubosidad": data["clouds"]["all"],
            "viento_velocidad": data.get("wind", {}).get("speed", 0),
            "viento_direccion": data.get("wind", {}).get("deg", 0),
            "visibilidad": data.get("visibility", 0),
            "amanecer": datetime.fromtimestamp(data["sys"]["sunrise"]).strftime("%H:%M:%S"),
            "atardecer": datetime.fromtimestamp(data["sys"]["sunset"]).strftime("%H:%M:%S"),
            "coordenadas": {
                "latitud": data["coord"]["lat"],
                "longitud": data["coord"]["lon"],
            },
            "timestamp": datetime.now().isoformat(),
        }
        logger.info(
            "event=transform_success ciudad=%s temperatura=%s",
            unicodedata.normalize("NFKD", registro_clima["ciudad"]).encode("ascii", "ignore").decode(),
            registro_clima["temperatura"],
        )
        return registro_clima
    except Exception:
        logger.exception("event=transform_error")
        raise
    finally:
        logger.info("event=transform_time seconds=%.3f", perf_counter() - start)


@task(name="guardar_json", retries=2, retry_delay_seconds=2)
def guardar_json(registro_clima: Dict[str, Any], json_path: str) -> str:
    logger = get_run_logger()
    start = perf_counter()
    try:
        logger.info("event=save_json_start path=%s", json_path)
        with open(json_path, "w", encoding="utf-8") as json_file:
            json.dump(registro_clima, json_file, ensure_ascii=False, indent=2)
        logger.info("event=save_json_success path=%s", json_path)
        return json_path
    except Exception:
        logger.exception("event=save_json_error path=%s", json_path)
        raise
    finally:
        logger.info("event=save_json_time path=%s seconds=%.3f", json_path, perf_counter() - start)


@task(name="leer_json", retries=2, retry_delay_seconds=2)
def leer_json(json_path: str) -> Dict[str, Any]:
    logger = get_run_logger()
    start = perf_counter()
    try:
        logger.info("event=read_json_start path=%s", json_path)
        with open(json_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        if not isinstance(data, dict) or not data:
            raise ValueError("El JSON leído está vacío o no tiene formato de objeto.")
        logger.info("event=read_json_success path=%s", json_path)
        return data
    except Exception:
        logger.exception("event=read_json_error path=%s", json_path)
        raise
    finally:
        logger.info("event=read_json_time path=%s seconds=%.3f", json_path, perf_counter() - start)


@task(
    name="cargar_a_mongo",
    retries=3,  # Retry para fallos temporales de conexión a MongoDB
    retry_delay_seconds=5,
)
def cargar_a_mongo(data_from_json: Dict[str, Any]) -> str:
    logger = get_run_logger()
    start = perf_counter()
    client = None
    try:
        logger.info("event=load_start db=%s collection=%s", DB_NAME, COLLECTION_NAME)
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        result = collection.insert_one(data_from_json)
        logger.info("event=load_success inserted_id=%s", result.inserted_id)
        return f"Documento insertado en MongoDB con _id={result.inserted_id}"
    except Exception:
        logger.exception("event=load_error")
        raise
    finally:
        if client is not None:
            client.close()
        logger.info("event=load_time seconds=%.3f", perf_counter() - start)


@flow(name="etl-clima-openweather-mongo")
def clima_flow(ciudad: str = DEFAULT_CITY, json_path: str = "clima_data.json") -> None:
    logger = get_run_logger()
    flow_start = perf_counter()
    try:
        logger.info("event=flow_start ciudad=%s json_path=%s", ciudad, json_path)

        # ETL: Extract
        data = extraer_datos_clima(ciudad)
        # ETL: Transform
        registro_clima = transformar_datos(data)
        # ETL: Load (archivo intermedio + MongoDB)
        ruta_json = guardar_json(registro_clima, json_path)
        data_from_json = leer_json(ruta_json)
        resultado = cargar_a_mongo(data_from_json)

        logger.info(
            "event=flow_success ciudad=%s temperatura=%s resultado_load=%s",
            unicodedata.normalize("NFKD", registro_clima["ciudad"]).encode("ascii", "ignore").decode(),
            registro_clima["temperatura"],
            resultado,
        )
    except Exception:
        # Excepción propagada para que Prefect marque FAILED correctamente.
        logger.exception("event=flow_error")
        raise
    finally:
        logger.info("event=flow_time seconds=%.3f", perf_counter() - flow_start)


if __name__ == "__main__":
    clima_flow()
