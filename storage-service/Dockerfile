# 1. Usar una imagen base oficial de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo
WORKDIR /app

# 3. Copiar e instalar requerimientos
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el código de la aplicación y script SQL (por si se requiere en un init job aparte)
COPY main.py .
COPY init.sql ./init.sql

# 5. Exponer el puerto
EXPOSE 8000

# 6. Comando de inicio
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
