import json
import time
import pika
import torch
import logging
import numpy as np
from sequencer import segment_text
from sentence_transformers import SentenceTransformer
from logger import logger
from config import RABBITMQ_HOST, VECTORIZATION_QUEUE, INDEXING_QUEUE, RABBITMQ_RETRY_DELAY, RABBITMQ_USER, RABBITMQ_PASSWORD, MACHINE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

device = "mps" if torch.backends.mps.is_available() else "cpu"
model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

time_encode = 0
time_get_rabbit_connection = 0

def vectorize_text(segments: list[str]) -> np.ndarray:
    """Vectorise une liste de segments de texte.

    :param list[str] segments: segments à encoder
    :return: embedding moyen normalisé
    :rtype: numpy.ndarray
    """
    global time_encode  # track encoding time
    start_time = time.time()
    embeddings = []
    # Vectorize each segment
    for segment in segments:

        embedding = model.encode(segment)
        embeddings.append(embedding)

    # Compute the mean embedding
    mean_embedding = np.mean(embeddings, axis=0)

    # Normalize the mean embedding fot get ready for indexing
    normalized_mean_embedding = mean_embedding / np.linalg.norm(mean_embedding)
    time_encode = time.time() - start_time
    return normalized_mean_embedding

def get_rabbit_connection() -> pika.BlockingConnection:
    """Établit une connexion RabbitMQ et mesure la durée.

    :return: connexion ouverte
    :rtype: pika.BlockingConnection
    """
    global time_get_rabbit_connection  # track connection time
    start_time = time.time()
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            logging.info("Connecté à RabbitMQ")
            time_get_rabbit_connection = time.time() - start_time
            return connection
        except Exception as e:
            logging.error(f"Erreur de connexion à RabbitMQ: {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes.")
            time.sleep(RABBITMQ_RETRY_DELAY)

def callback(ch, method, properties, body) -> None:
    """Traite un message de texte, le vectorise et publie l'embedding.

    :param ch: canal RabbitMQ
    :param method: meta-données de livraison
    :param properties: propriétés AMQP
    :param body: contenu JSON encodé
    :return: ``None``
    :rtype: None
    """
    try:
        message = json.loads(body)
        text = message['text']
        segments = segment_text(text, 150, 2)
        embedding = vectorize_text(segments)  # retourne un numpy array
        new_message = {
            "url": message["url"],
            "h1": message["h1"],
            "embedding": embedding.tolist()  # conversion pour JSON
        }
        ch.basic_publish(
            exchange='',
            routing_key=INDEXING_QUEUE,
            body=json.dumps(new_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logging.info(f"Vectorisation terminée pour {message['url']}")
        data = {
            "step": "vector",
            "url": message["url"],
            "h1": message["h1"],
            "time_encode": time_encode,
            "time_get_rabbit_connection": time_get_rabbit_connection,
            "computer": MACHINE,
        }
        logger(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Erreur dans le callback de vectorisation pour le message {body}: {e}")
        # En cas d'erreur, on peut requeue le message pour réessayer
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main() -> None:
    """Démarre le consumer de vectorisation CPU.

    :return: ``None``
    :rtype: None
    """
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=VECTORIZATION_QUEUE, on_message_callback=callback)
    logging.info("Vectorizer Consumer en attente de messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interruption manuelle, arrêt du consumer.")
        channel.stop_consuming()
    except Exception as e:
        logging.error(f"Erreur dans le consumer: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
