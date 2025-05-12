# config.py

# RabbitMQ
RABBITMQ_HOST = '158.69.54.81'
# RABBITMQ_HOST = 'localhost'
RABBITMQ_USER = 'cocosjn'
RABBITMQ_PASSWORD = 'cocosjn'
VECTORIZATION_QUEUE = 'vectorization_queue'
INDEXING_QUEUE = 'indexing_queue'
DOWNLOAD_QUEUE = 'download_queue'
RABBITMQ_RETRY_DELAY = 5  # secondes avant tentative de reconnexion
MAX_WORKERS = 4
# Elasticsearch
ES_HOSTS = [{"host":'158.69.54.81',"port":9200}]
# ES_HOSTS = [{"host":'localhost',"port":9200}]
ES_INDEX = 'pages'
ES_DIMS = 384  # Dimension des embeddings pour le modèle "all-MiniLM-L6-v2"

# Paramètres généraux
LOG_LEVEL = 'INFO'

MONGO_USER = "root"
MONGO_PASS = "password"
MONGO_HOST = "158.69.54.81"
MONGO_PORT = 27017
MONGO_AUTH_SRC = "admin" 