# Imagen base ligera de Python
FROM python:3.11-slim

# Evitar que Python genere .pyc y buffering raro
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la app
COPY app/ app/

# Puerto donde exponemos la API
EXPOSE 8000

# Comando para arrancar la API con Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
