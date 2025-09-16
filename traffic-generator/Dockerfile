# 1. Usar una imagen base oficial de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar el archivo de requerimientos e instalarlos
# Se copia primero para aprovechar el caché de Docker si no cambian
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el resto del código de la aplicación y los datos
COPY main.py .
# IMPORTANTE: Este COPY asume que el build context incluye la carpeta data en la raíz.
# Al construir debes situarte en la raíz del proyecto y hacer:
# docker build -t traffic-generator -f traffic-generator/Dockerfile .
# Así el siguiente COPY funcionará:
COPY data ./data

# 5. Comando que se ejecutará cuando el contenedor se inicie
CMD ["python", "main.py"]
