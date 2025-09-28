# 1. Usar una imagen base oficial de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar el archivo de requerimientos e instalarlos
# Se copia primero para aprovechar el caché de Docker si no cambian
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el resto del código de la aplicación
COPY main.py .

# (Opcional) Copiar datos si existen en el contexto. Si no existe la carpeta, no falla si usamos un build ARG toggle.
ARG INCLUDE_DATA=false
RUN mkdir -p ./data

# 5. Comando que se ejecutará cuando el contenedor se inicie
CMD ["python", "main.py"]
