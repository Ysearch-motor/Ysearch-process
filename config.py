# config.py

# RabbitMQ
RABBITMQ_HOST = 'localhost'
VECTORIZATION_QUEUE = 'vectorization_queue'
INDEXING_QUEUE = 'indexing_queue'
RABBITMQ_RETRY_DELAY = 5  # secondes avant tentative de reconnexion

# Elasticsearch
ES_HOSTS = ['localhost:9200']
ES_INDEX = 'pages'
ES_DIMS = 384  # Dimension des embeddings pour le modèle "all-MiniLM-L6-v2"

# Paramètres généraux
LOG_LEVEL = 'INFO'
