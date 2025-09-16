"""Script rápido para probar el modelo Gemini y el cálculo de similitud.
Ejecutar solo para validar credenciales y dependencias.
Se puede eliminar luego de la verificación.

Uso:
    python test_model.py

Requisitos:
    - Archivo .env con GEMINI_API_KEY
"""
import os
from dotenv import load_dotenv
import google.generativeai as genai
from sentence_transformers import SentenceTransformer, util

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-pro")
if not api_key:
    raise SystemExit("Falta GEMINI_API_KEY en .env")

print("Configurando Gemini...")
genai.configure(api_key=api_key)

print(f"Creando modelo generativo: {model_name}")
model = genai.GenerativeModel(model_name)

prompt = "Explica brevemente qué es un sistema distribuido."
print("Generando contenido de prueba...")
response = model.generate_content(prompt)
print("Respuesta del modelo (truncada):", getattr(response, 'text', '')[:180], '...')

print("Cargando modelo de embeddings...")
emb_model = SentenceTransformer('all-MiniLM-L6-v2')

text_a = "Un sistema distribuido es un conjunto de computadoras que coopera para lograr un objetivo común."
text_b = getattr(response, 'text', '')

if not text_b:
    print("El modelo no devolvió texto utilizable para la similitud.")
else:
    emb_a = emb_model.encode(text_a, convert_to_tensor=True)
    emb_b = emb_model.encode(text_b, convert_to_tensor=True)
    score = util.pytorch_cos_sim(emb_a, emb_b).item()
    print(f"Score de similitud entre texto humano y respuesta LLM: {score:.4f}")

print("Prueba completada.")
