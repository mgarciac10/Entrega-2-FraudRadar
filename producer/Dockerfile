# Imagen del productor. Se construye desde la raíz del repositorio:
#   docker build -f producer/Dockerfile -t fraudradar-producer .
FROM python:3.12-slim

WORKDIR /app
COPY producer/requirements.txt ./producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

# El productor importa los umbrales de spark/common/reglas.py
COPY producer ./producer
COPY spark/common ./spark/common

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "producer/producer.py"]
