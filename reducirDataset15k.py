import pandas as pd
import os

# --- Configuración ---
# Nombre del archivo CSV original y completo que descargaste de Kaggle.
NOMBRE_ARCHIVO_ORIGINAL = 'train.csv'

# Nombre que tendrá el nuevo archivo con el dataset reducido.
NOMBRE_ARCHIVO_REDUCIDO = 'train_15k.csv'

# Directorio donde se guardará el nuevo archivo.
# Se creará si no existe.
DIRECTORIO_SALIDA = 'data'

# Número de filas aleatorias que quieres en tu nuevo dataset.
NUMERO_DE_FILAS = 15000

# Semilla aleatoria para asegurar que la muestra sea siempre la misma.
# Esto es útil para que tus experimentos sean reproducibles.
SEMILLA_ALEATORIA = 42

# --- Lógica del Script ---

def procesar_dataset():
    """
    Función principal que carga, reduce, limpia y guarda el dataset.
    """
    # 1. Verificar si el archivo original existe
    if not os.path.exists(NOMBRE_ARCHIVO_ORIGINAL):
        print(f"Error: El archivo '{NOMBRE_ARCHIVO_ORIGINAL}' no se encuentra.")
        print("Por favor, asegúrate de que el script esté en la misma carpeta que el dataset.")
        return

    print(f"Cargando el dataset original '{NOMBRE_ARCHIVO_ORIGINAL}'... Esto puede tardar unos minutos.")

    # 2. Cargar el dataset completo en un DataFrame de pandas
    try:
        df_full = pd.read_csv(NOMBRE_ARCHIVO_ORIGINAL, header=None)
        # Asignar nombres a las columnas según la descripción de la tarea
        df_full.columns = ['class_index', 'question_title', 'question_content', 'best_answer']
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo CSV: {e}")
        return

    print("Dataset cargado exitosamente.")
    print(f"Número total de filas original: {len(df_full)}")

    # 3. Tomar una muestra aleatoria del número de filas especificado
    print(f"Generando una muestra aleatoria de {NUMERO_DE_FILAS} filas...")
    df_sample = df_full.sample(n=NUMERO_DE_FILAS, random_state=SEMILLA_ALEATORIA)

    # 4. Limpieza básica: eliminar filas donde la pregunta o la respuesta estén vacías
    # Esto previene errores en los servicios que consumirán los datos.
    filas_antes_limpieza = len(df_sample)
    df_sample.dropna(subset=['question_title', 'best_answer'], inplace=True)
    filas_despues_limpieza = len(df_sample)
    
    filas_eliminadas = filas_antes_limpieza - filas_despues_limpieza
    if filas_eliminadas > 0:
        print(f"Limpieza: Se eliminaron {filas_eliminadas} filas con valores nulos en pregunta o respuesta.")

    # 5. Crear el directorio de salida si no existe
    if not os.path.exists(DIRECTORIO_SALIDA):
        print(f"Creando el directorio de salida: '{DIRECTORIO_SALIDA}'")
        os.makedirs(DIRECTORIO_SALIDA)

    # 6. Guardar el nuevo DataFrame reducido en un archivo CSV
    ruta_archivo_final = os.path.join(DIRECTORIO_SALIDA, NOMBRE_ARCHIVO_REDUCIDO)
    print(f"Guardando el dataset reducido en '{ruta_archivo_final}'...")
    
    # index=False evita que pandas añada una columna de índice extra al CSV
    df_sample.to_csv(ruta_archivo_final, index=False)

    print("\n¡Proceso completado con éxito!")
    print(f"Resumen:")
    print(f"  - Se ha creado el archivo '{ruta_archivo_final}'.")
    print(f"  - Contiene {len(df_sample)} filas limpias y listas para usar.")

if __name__ == "__main__":
    procesar_dataset()