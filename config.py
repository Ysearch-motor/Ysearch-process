import os
import ast
from pathlib import Path
from dotenv import load_dotenv

# Charge le .env dans le même dossier que ce fichier
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# RabbitMQ
RABBITMQ_HOST        = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER        = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD    = os.getenv("RABBITMQ_PASSWORD")
VECTORIZATION_QUEUE  = os.getenv("VECTORIZATION_QUEUE")
INDEXING_QUEUE       = os.getenv("INDEXING_QUEUE")
DOWNLOAD_QUEUE       = os.getenv("DOWNLOAD_QUEUE")
RABBITMQ_RETRY_DELAY = int(os.getenv("RABBITMQ_RETRY_DELAY", 5))
MAX_WORKERS          = int(os.getenv("MAX_WORKERS", 1))

# Elasticsearch
ES_HOSTS = ast.literal_eval(os.getenv("ES_HOSTS", "[]"))
ES_INDEX = os.getenv("ES_INDEX")
ES_DIMS  = int(os.getenv("ES_DIMS", 384))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# MongoDB
MONGO_USER     = os.getenv("MONGO_USER")
MONGO_PASS     = os.getenv("MONGO_PASS")
MONGO_HOST     = os.getenv("MONGO_HOST")
MONGO_PORT     = int(os.getenv("MONGO_PORT", 27017))
MONGO_AUTH_SRC = os.getenv("MONGO_AUTH_SRC")

# Machine
MACHINE = os.getenv("MACHINE", "Mac Valentin")
