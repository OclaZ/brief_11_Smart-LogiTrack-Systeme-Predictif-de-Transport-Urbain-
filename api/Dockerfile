# api/Dockerfile
FROM python:3.10-slim-bullseye

# 1. Installation de Java (Obligatoire pour PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# 2. Copie des requirements
COPY requirements.txt .

# 3. Installation des dépendances avec mise à jour de PIP et Timeout augmenté
# On upgrade pip d'abord, puis on installe avec un timeout de 1000 secondes
RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

# 4. Copie du code
COPY . .

# 5. Lancement
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]