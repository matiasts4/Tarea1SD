import pandas as pd
import requests
import time
import os
import random

# --- Configuración ---
# La URL del servicio de caché. Se obtiene de una variable de entorno para flexibilidad.
# Valor por defecto para pruebas locales sin Docker.
CACHE_SERVICE_URL = os.getenv('CACHE_SERVICE_URL', 'http://localhost:8001/query')

# Ruta al archivo de datos (relativa al root del proyecto).
DATASET_PATH = 'data/train_15k.csv'

# Tiempo de espera (en segundos) entre cada envío de pregunta.
SLEEP_TIME = float(os.getenv('SLEEP_TIME', '1.5'))

# Porcentaje de preguntas que serán repeticiones de un pool popular (para generar hits)
# Valor entre 0.0 (sin repeticiones) y 1.0 (solo repeticiones)
REPEAT_PROBABILITY = float(os.getenv('REPEAT_PROBABILITY', '0.5'))  # 50% por defecto

# Tamaño del pool de preguntas "populares" que se repetirán frecuentemente
POPULAR_POOL_SIZE = int(os.getenv('POPULAR_POOL_SIZE', '200'))


def start_traffic_generator():
    """
    Inicia el generador de tráfico, leyendo el dataset y enviando preguntas
    al servicio de caché de forma continua.
    """
    print("--- Iniciando Generador de Tráfico ---")

    # 1. Cargar el dataset
    try:
        df = pd.read_csv(DATASET_PATH)
        # Asegurarnos de que no hay filas vacías que puedan causar problemas
        df.dropna(subset=['question_title', 'best_answer'], inplace=True)
        print(f"Dataset '{DATASET_PATH}' cargado exitosamente con {len(df)} filas.")
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de datos en '{DATASET_PATH}'.")
        print("Asegúrate de que la ruta es correcta y el archivo existe.")
        return
    except Exception as e:
        print(f"Error inesperado leyendo el dataset: {e}")
        return

    # 2. Crear un pool de preguntas "populares" que se repetirán frecuentemente
    popular_questions = df.sample(n=min(POPULAR_POOL_SIZE, len(df))).copy()
    print(f"Pool de preguntas populares creado: {len(popular_questions)} preguntas")
    print(f"Probabilidad de repetición: {REPEAT_PROBABILITY*100:.1f}%")
    
    # 3. Bucle infinito para enviar tráfico constantemente
    print(f"Enviando preguntas a: {CACHE_SERVICE_URL}")
    while True:
        try:
            # Decidir si usar una pregunta del pool popular (para generar hits) o una nueva
            if random.random() < REPEAT_PROBABILITY:
                # Usar una pregunta del pool popular (más probable que genere hit)
                random_row = popular_questions.sample(n=1).iloc[0]
            else:
                # Usar una pregunta aleatoria del dataset completo (miss seguro)
                random_row = df.sample(n=1).iloc[0]
            
            question = random_row['question_title']
            answer = random_row['best_answer']

            # Crear el cuerpo de la petición JSON
            payload = {
                'question': question,
                'original_answer': answer
            }

            print(f"\nEnviando pregunta: '{question[:80]}...'")

            # 4. Enviar la petición HTTP POST al servicio de caché
            response = requests.post(CACHE_SERVICE_URL, json=payload, timeout=5)  # Timeout de 5s

            # 5. Imprimir la respuesta del servidor
            if response.status_code == 200:
                # Intentar parsear JSON
                try:
                    print(f"Respuesta recibida (200 OK): {response.json()}")
                except ValueError:
                    print(f"Respuesta recibida (200 OK) pero no es JSON: {response.text}")
            else:
                print(f"Error del servidor ({response.status_code}): {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Error de conexión: No se pudo conectar al servicio de caché. {e}")
            print("¿Está el servicio de caché corriendo?")
        except KeyError as e:
            print(f"Columnas esperadas no encontradas en el dataset: {e}")
            break
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")

        # 6. Esperar antes de enviar la siguiente pregunta
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    # Esperamos unos segundos para dar tiempo a que los otros servicios se inicien (en un entorno Docker)
    initial_wait = int(os.getenv('INITIAL_WAIT_SECONDS', '10'))
    print(f"Generador de Tráfico esperando {initial_wait} segundos antes de iniciar...")
    time.sleep(initial_wait)
    start_traffic_generator()
