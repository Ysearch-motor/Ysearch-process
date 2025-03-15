FROM python:3.12-slim

WORKDIR /app

# Copier et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source
COPY . .

# Le CMD par défaut sera remplacé par la commande spécifique de chaque service dans docker-compose.yml
CMD ["python", "main.py"]
