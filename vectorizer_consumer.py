import json
import time
import logging
import pika
from sequencer import segment_text
from vectoriser import vectorize_text
from config import RABBITMQ_HOST, VECTORIZATION_QUEUE, INDEXING_QUEUE, RABBITMQ_RETRY_DELAY, RABBITMQ_USER, RABBITMQ_PASSWORD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_rabbit_connection():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            logging.info("Connecté à RabbitMQ")
            return connection
        except Exception as e:
            logging.error(f"Erreur de connexion à RabbitMQ: {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes.")
            time.sleep(RABBITMQ_RETRY_DELAY)

def callback(ch, method, properties, body):
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
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Erreur dans le callback de vectorisation pour le message {body}: {e}")
        # En cas d'erreur, on peut requeue le message pour réessayer
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
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
