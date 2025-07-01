import json
import time
import logging
import threading
import pika
from opensearchpy import OpenSearch, helpers
from logger import logger
from config import (
    RABBITMQ_HOST, INDEXING_QUEUE,
    ES_HOSTS, ES_INDEX, ES_DIMS,
    RABBITMQ_RETRY_DELAY, RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    MACHINE
)

# ------------------------------------
# Ajuste ici la taille de ton batch.
# 100, 1000 ou plus : un batch plus grand réduit la surcharge par message.
BATCH_SIZE = 1000
# ------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

time_es_connection = 0
time_indexation = 0
time_rabbitmq_connection = 0
time_indexation_lock = threading.Lock()


def get_es_connection() -> OpenSearch:
    """Établit une connexion à OpenSearch.

    Returns:
        OpenSearch: client connecté.
    """
    global time_es_connection
    while True:
        try:
            start = time.time()
            es = OpenSearch(
                hosts=ES_HOSTS,
                http_compress=True,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True,
            )
            if es.ping():
                time_es_connection = time.time() - start
                logging.info(
                    f"Connecté à OpenSearch en {time_es_connection:.3f}s"
                )
                return es
            raise Exception("Ping failed")
        except Exception as e:
            logging.error(
                f"Erreur connexion OpenSearch: {e}. "
                f"Nouvel essai dans {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)


def create_index(es: OpenSearch) -> None:
    """Crée l'index OpenSearch s'il n'existe pas."""
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


def get_rabbit_connection() -> pika.BlockingConnection:
    """Retourne une connexion RabbitMQ.

    Returns:
        pika.BlockingConnection: connexion ouverte.
    """
    global time_rabbitmq_connection
    start = time.time()
    while True:
        try:
            creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=creds)
            )
            time_rabbitmq_connection = time.time() - start
            logging.info(
                f"Connecté à RabbitMQ en {time_rabbitmq_connection:.3f}s"
            )
            return conn
        except Exception as e:
            logging.error(
                f"Erreur connexion RabbitMQ: {e}. "
                f"Nouvel essai dans {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)


def background_bulk(
    es_client: OpenSearch, docs: list[dict], batch_size: int
) -> None:
    """Indexe un lot de documents dans un thread séparé."""
    global time_indexation
    start_batch = time.time()
    try:
        helpers.bulk(es_client, docs)
        batch_time = time.time() - start_batch

        # Met à jour de façon thread-safe le temps d'indexation cumulé
        with time_indexation_lock:
            time_indexation += batch_time
            cumulative_time = time_indexation

        data = {
            "step": "index_batch_async",
            "batchsize": batch_size,
            "batch_time": batch_time,
            "cumulative_index_time": cumulative_time,
            "time_rabbitmq_connection": time_rabbitmq_connection,
            "time_es_connection": time_es_connection,
            "machine": MACHINE
        }
        logger(data)
        logging.info(f"Batch async de {batch_size} docs indexé en {batch_time:.3f}s")
    except Exception as e:
        logging.error(f"Erreur bulk async: {e}")
        # Ici, les documents sont déjà ackés, donc ils sont perdus en cas d'erreur.


def main() -> None:
    """Consomme les messages de vecteurs et les indexe par lots."""
    es = get_es_connection()
    create_index(es)

    actions = []
    delivery_tags = []

    def callback(ch, method, properties, body):
        nonlocal actions, delivery_tags

        try:
            msg = json.loads(body)
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

            if len(actions) >= BATCH_SIZE:
                # Snapshot du batch à envoyer
                docs_to_send = actions.copy()
                batch_count = len(docs_to_send)

                # Récupère le dernier delivery_tag
                last_tag = delivery_tags[-1]

                # Réinitialisation immédiate avant de lancer l'index async
                actions.clear()
                delivery_tags.clear()

                # Ack multiple pour libérer RabbitMQ
                ch.basic_ack(delivery_tag=last_tag, multiple=True)

                # Lancer le bulk en arrière-plan
                thread = threading.Thread(
                    target=background_bulk,
                    args=(es, docs_to_send, batch_count),
                    daemon=True
                )
                thread.start()

        except Exception as e:
            logging.error(f"Erreur traitement message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=BATCH_SIZE)
    channel.basic_consume(queue=INDEXING_QUEUE, on_message_callback=callback)

    logging.info("Consumer en attente de messages...")
    try:
        channel.start_consuming()

    except KeyboardInterrupt:
        logging.info("Interruption manuelle, arrêt du consumer.")

    finally:
        # Flush final pour les messages restants (< BATCH_SIZE)
        if actions:
            docs_to_send = actions.copy()
            batch_count = len(docs_to_send)
            last_tag = delivery_tags[-1]

            # Ack avant le flush final
            channel.basic_ack(delivery_tag=last_tag, multiple=True)
            actions.clear()
            delivery_tags.clear()

            # Lancer le flush final en arrière-plan
            thread = threading.Thread(
                target=background_bulk,
                args=(es, docs_to_send, batch_count),
                daemon=True
            )
            thread.start()

            # Attendre brièvement que le thread démarre (optionnel)
            time.sleep(0.1)

        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
