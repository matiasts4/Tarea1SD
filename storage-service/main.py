import os
import time
from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

# --- Configuración de la Base de Datos ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/yahoo_db")
MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", 10))
RETRY_DELAY = int(os.getenv("DB_RETRY_DELAY", 5))

engine = None
for i in range(MAX_RETRIES):
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Conexión a la base de datos establecida exitosamente.")
        break
    except OperationalError as e:
        print(f"Intento {i+1}/{MAX_RETRIES}: No se pudo conectar a la base de datos. Reintentando en {RETRY_DELAY}s...")
        print(f"Error: {e}")
        time.sleep(RETRY_DELAY)
else:
    print("Error crítico: No se pudo conectar a la base de datos después de varios intentos.")
    raise SystemExit(1)

app = FastAPI(title="Storage Service")

@app.post("/storage")
async def store_response(request: Request):
    payload = await request.json()
    question = payload.get("question")
    if not question:
        raise HTTPException(status_code=400, detail="'question' es obligatoria")

    stmt = text("""
        INSERT INTO responses (question, original_answer, llm_answer, score)
        VALUES (:question, :original_answer, :llm_answer, :score)
        ON CONFLICT (question) DO NOTHING;
    """)

    try:
        with engine.begin() as connection:  # begin() maneja commit/rollback
            connection.execute(stmt, payload)
        print(f"Dato guardado para la pregunta: '{question[:80]}...'")
        return {"status": "success", "message": "Datos almacenados correctamente."}
    except Exception as e:
        print(f"Error al guardar en la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error al interactuar con la base de datos.")

@app.post("/hit")
async def register_hit(request: Request):
    payload = await request.json()
    question = payload.get("question")
    if not question:
        raise HTTPException(status_code=400, detail="'question' es obligatoria")

    stmt = text("""
        UPDATE responses
        SET hit_count = hit_count + 1
        WHERE question = :question;
    """)

    try:
        with engine.begin() as connection:
            result = connection.execute(stmt, {"question": question})
        print(f"HIT registrado para la pregunta: '{question[:80]}...'")
        return {"status": "success", "message": "Hit registrado."}
    except Exception as e:
        print(f"Error al registrar hit: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar el contador de hits.")

@app.get("/")
async def root():
    return {"message": "Storage Service está funcionando y conectado a la base de datos."}

@app.get("/health")
async def health():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
