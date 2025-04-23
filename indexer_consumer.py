import json
import time
import logging
import pika
from opensearchpy import OpenSearch, helpers
from config import (
    RABBITMQ_HOST, INDEXING_QUEUE,
    ES_HOSTS, ES_INDEX, ES_DIMS,
    RABBITMQ_RETRY_DELAY, RABBITMQ_USER,
    RABBITMQ_PASSWORD
)

# Taille maximale du batch avant envoi
BATCH_SIZE = 10000

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_es_connection():
    while True:
        try:
            es = OpenSearch(
                hosts=ES_HOSTS,
                http_compress=True,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True
            )
            if es.ping():
                logging.info("Connecté à OpenSearch")
                return es
            raise Exception("Ping failed")
        except Exception as e:
            logging.error(
                f"Erreur de connexion à OpenSearch: {e}. "
                f"Nouvelle tentative dans {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)

def create_index(es):
    if not es.indices.exists(index=ES_INDEX):
        mapping = {
            "settings": {
                "index": {
                    "knn": True,
                    "knn.algo_param.ef_search": 512,
                    "knn.algo_param.ef_construction": 512,
                    "knn.algo_param.m": 16
                }
            },
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "h1": {"type": "text"},
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": ES_DIMS
                    }
                }
            }
        }
        try:
            es.indices.create(index=ES_INDEX, body=mapping)
            logging.info(f"Index {ES_INDEX} créé.")
        except Exception as e:
            logging.error(f"Erreur création index {ES_INDEX}: {e}")

def get_rabbit_connection():
    while True:
        try:
            creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST,
                                          credentials=creds)
            )
            logging.info("Connecté à RabbitMQ")
            return conn
        except Exception as e:
            logging.error(
                f"Erreur connexion RabbitMQ: {e}. "
                f"Nouveau essai dans {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)

def main():
    es = get_es_connection()
    create_index(es)

    # Tampons d'accumulation
    actions = []
    delivery_tags = []

    def callback(ch, method, properties, body):
        nonlocal actions, delivery_tags

        try:
            msg = json.loads(body)
            # Préparation de l’action pour bulk
            action = {
                "_index": ES_INDEX,
                "_source": {
                    "url": msg["url"],
                    "h1": msg["h1"],
                    "embedding": msg["embedding"]
                }
            }
            actions.append(action)
            delivery_tags.append(method.delivery_tag)

            # Dès qu'on atteint la taille de batch, on envoie
            if len(actions) >= BATCH_SIZE:
                helpers.bulk(es, actions)
                logging.info(f"Batch de {len(actions)} documents indexé.")
                # Ack après succès
                for tag in delivery_tags:
                    ch.basic_ack(delivery_tag=tag)
                # Réinitialisation des tampons
                actions, delivery_tags = [], []

        except Exception as e:
            logging.error(f"Erreur durant le traitement du message {body}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
    # On peut augmenter prefetch_count à BATCH_SIZE pour plus d'efficacité
    channel.basic_qos(prefetch_count=BATCH_SIZE)
    channel.basic_consume(queue=INDEXING_QUEUE, on_message_callback=callback)

    logging.info("Indexer Consumer en attente de messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interruption manuelle, arrêt du consumer.")
    finally:
        # Avant de quitter, on flush tout ce qui reste
        if actions:
            try:
                helpers.bulk(es, actions)
                logging.info(f"Flush final de {len(actions)} docs.")
                for tag in delivery_tags:
                    channel.basic_ack(delivery_tag=tag)
            except Exception as e:
                logging.error(f"Erreur flush final: {e}")
        channel.stop_consuming()
        connection.close()

if __name__ == "__main__":
    main()
