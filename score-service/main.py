import os
from fastapi import FastAPI, Request, HTTPException
from sentence_transformers import SentenceTransformer, util
import google.generativeai as genai
import requests
from dotenv import load_dotenv

# Cargar variables desde .env si existe
load_dotenv()

# --- Configuración ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("La variable de entorno GEMINI_API_KEY no ha sido configurada.")

genai.configure(api_key=GEMINI_API_KEY)

STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://localhost:8003/storage")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash-lite")

print("Cargando el modelo de similitud de sentencias (puede tardar)...")
similarity_model = SentenceTransformer('all-MiniLM-L6-v2')
print("Modelo de similitud cargado.")

# Intentar inicializar el modelo generativo
try:
    llm = genai.GenerativeModel(GEMINI_MODEL_NAME)
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar el modelo Gemini '{GEMINI_MODEL_NAME}': {e}")

app = FastAPI(title="Score Service")


def get_llm_answer(question: str) -> str:
    """Envía la pregunta a la API de Gemini y retorna la respuesta generada."""
    try:
        prompt = f"Responde la siguiente pregunta de la forma más clara y concisa posible: {question}"
        response = llm.generate_content(prompt)
        return getattr(response, 'text', 'Sin texto devuelto por LLM')
    except Exception as e:
        print(f"Error al contactar la API de Gemini: {e}")
        return "Error: No se pudo generar una respuesta desde el LLM."


def calculate_similarity(text1: str, text2: str) -> float:
    """Calcula similitud de coseno entre dos textos usando embeddings."""
    embedding1 = similarity_model.encode(text1, convert_to_tensor=True)
    embedding2 = similarity_model.encode(text2, convert_to_tensor=True)
    cosine_score = util.pytorch_cos_sim(embedding1, embedding2)
    return cosine_score.item()


@app.post("/score")
async def handle_scoring(request: Request):
    """Recibe pregunta y respuesta original, genera respuesta LLM y calcula score."""
    try:
        payload = await request.json()
        question = payload.get("question")
        original_answer = payload.get("original_answer")

        if not all([question, original_answer]):
            raise HTTPException(status_code=400, detail="Faltan 'question' u 'original_answer'.")

        print(f"\nRecibida pregunta para scoring: '{question[:80]}...'")
        print("Generando respuesta con el LLM...")
        llm_answer = get_llm_answer(question)
        print(f"Respuesta del LLM (truncada): '{llm_answer[:80]}...'")

        print("Calculando score de similitud...")
        score = calculate_similarity(original_answer, llm_answer)
        print(f"Score de similitud: {score:.4f}")

        result_payload = {
            "question": question,
            "original_answer": original_answer,
            "llm_answer": llm_answer,
            "score": score
        }

        # Enviar al storage service
        try:
            storage_response = requests.post(STORAGE_SERVICE_URL, json=result_payload, timeout=5)
            storage_response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error: No se pudo conectar con el Storage Service. {e}")
            raise HTTPException(status_code=503, detail="El servicio de almacenamiento no está disponible.")

        return result_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error procesando la petición de scoring: {e}")
        raise HTTPException(status_code=500, detail="Error interno en el servidor de score.")


@app.get("/")
def read_root():
    return {"message": "Score Service está funcionando correctamente."}
