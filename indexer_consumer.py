import json
import time
import logging
import pika
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
from config import RABBITMQ_HOST, INDEXING_QUEUE, ES_HOSTS, ES_INDEX, ES_DIMS, RABBITMQ_RETRY_DELAY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_es_connection():
    while True:
        try:
            es = Elasticsearch(hosts=ES_HOSTS)
            # Test de connexion
            if es.ping():
                logging.info("Connecté à Elasticsearch")
                return es
            else:
                raise ESConnectionError("Ping failed")
        except Exception as e:
            logging.error(f"Erreur de connexion à Elasticsearch: {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes.")
            time.sleep(RABBITMQ_RETRY_DELAY)

def create_index(es):
    if not es.indices.exists(index=ES_INDEX):
        mapping = {
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "h1": {"type": "text"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": ES_DIMS
                    }
                }
            }
        }
        try:
            es.indices.create(index=ES_INDEX, body=mapping)
            logging.info(f"Index {ES_INDEX} créé.")
        except Exception as e:
            logging.error(f"Erreur lors de la création de l'index {ES_INDEX}: {e}")

def get_rabbit_connection():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            logging.info("Connecté à RabbitMQ")
            return connection
        except Exception as e:
            logging.error(f"Erreur de connexion à RabbitMQ: {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes.")
            time.sleep(RABBITMQ_RETRY_DELAY)

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        doc = {
            "url": message["url"],
            "h1": message["h1"],
            "embedding": message["embedding"]
        }
        res = es.index(index=ES_INDEX, body=doc)
        logging.info(f"Document indexé (ID: {res.get('_id')}) pour {message['url']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Erreur lors de l'indexation du document {body}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    global es
    es = get_es_connection()
    create_index(es)

    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=INDEXING_QUEUE, on_message_callback=callback)
    logging.info("Indexer Consumer en attente de messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interruption manuelle, arrêt du consumer.")
        channel.stop_consuming()
    except Exception as e:
        logging.error(f"Erreur dans le consumer d'indexation: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
