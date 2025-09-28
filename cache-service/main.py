import os
from fastapi import FastAPI, Request, HTTPException
import requests
from collections import OrderedDict

# --- Configuración ---
SCORE_SERVICE_URL = os.getenv("SCORE_SERVICE_URL", "http://localhost:8002/score")
# Base URL del storage (sin sufijo /storage). El endpoint /hit y /storage se añaden explícitamente.
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://localhost:8003")

CACHE_SIZE = int(os.getenv("CACHE_SIZE", 1000))  # Tamaño máximo de la caché
CACHE_POLICY = os.getenv("CACHE_POLICY", "LRU")  # Política de desalojo (solo LRU soportada de momento)

# --- Implementación de la Caché (LRU) ---
cache = OrderedDict()

app = FastAPI(title="Cache Service")


def cache_put(question: str, value):
    """Inserta/actualiza un valor en la caché respetando la política LRU."""
    cache[question] = value
    cache.move_to_end(question)
    if len(cache) > CACHE_SIZE:
        oldest_item = cache.popitem(last=False)
        print(f"Caché llena. Eliminando el elemento más antiguo: '{oldest_item[0][:80]}...'")


def cache_get(question: str):
    """Obtiene un valor de la caché y actualiza su posición si existe."""
    if question in cache:
        cache.move_to_end(question)
        return cache[question]
    return None


@app.post("/query")
async def handle_query(request: Request):
    """
    Endpoint principal que recibe las preguntas del generador de tráfico.
    Verifica si la pregunta está en la caché (hit) o no (miss).
    """
    try:
        payload = await request.json()
        question = payload.get("question")

        if not question:
            raise HTTPException(status_code=400, detail="La clave 'question' es obligatoria.")

        print(f"\nRecibida pregunta: '{question[:80]}...'")

        # --- Lógica de la Caché ---
        cached_value = cache_get(question)
        if cached_value is not None:
            # CACHE HIT
            print("-> Cache HIT para la pregunta.")
            # Notificar (sin bloquear) al storage service
            try:
                requests.post(f"{STORAGE_SERVICE_URL}/hit", json={"question": question}, timeout=2)
            except requests.RequestException as e:
                print(f"Advertencia: No se pudo notificar el HIT al Storage Service. {e}")
            return {"status": "hit", "message": "Respuesta obtenida desde la caché.", "data": cached_value}
        else:
            # CACHE MISS
            print("-> Cache MISS. Enviando al Score Service...")
            try:
                response_from_score = requests.post(SCORE_SERVICE_URL, json=payload, timeout=15)
                response_from_score.raise_for_status()
                score_data = response_from_score.json()
                cache_put(question, score_data)
                return {"status": "miss", "data_from_score": score_data}
            except requests.RequestException as e:
                print(f"Error: No se pudo conectar con el Score Service. {e}")
                raise HTTPException(status_code=503, detail="El servicio de puntuación no está disponible.")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error procesando la petición: {e}")
        raise HTTPException(status_code=500, detail="Error interno en el servidor de caché.")


@app.get("/")
def read_root():
    return {"message": "Cache Service está funcionando correctamente.", "cache_size": len(cache)}


@app.get("/stats")
def cache_stats():
    """Devuelve estadísticas simples de la caché."""
    return {"size": len(cache), "capacity": CACHE_SIZE, "policy": CACHE_POLICY}
