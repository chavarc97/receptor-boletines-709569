# Usamos una imagen base de Python ligera
FROM python:3.10-slim

# Directorio de trabajo
WORKDIR /app

# Copiamos los requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el archivo de la app
COPY receptor.py .

# Comando para correr la app de forma interactiva (en background)
CMD ["python", "-u", "receptor.py"]
