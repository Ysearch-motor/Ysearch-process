import json
import time
import logging
import pika
import data_searcher as data_search
from config import RABBITMQ_HOST, VECTORIZATION_QUEUE, RABBITMQ_RETRY_DELAY, RABBITMQ_USER, RABBITMQ_PASSWORD

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_rabbit_connection() -> pika.BlockingConnection:
    """Établit une connexion RabbitMQ persistante."""
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            logging.info("Connecté à RabbitMQ")
            return connection
        except Exception as e:
            logging.error(
                f"Erreur de connexion à RabbitMQ : {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)

def main() -> None:
    """Lit des fichiers WARC locaux et publie le texte à vectoriser."""
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)

    # Lecture des chemins de fichiers WARC
    try:
        with open("path.paths", "r") as f:
            paths = f.readlines()
    except Exception as e:
        logging.error(f"Erreur de lecture du fichier path.paths: {e}")
        return

    warc_file = "./warc/fichier.warc.gz"

    for warcurl in paths:
        warcurl = warcurl.strip()
        try:
            records = data_search.get_data(warcurl, warc_file)
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction des données pour {warcurl}: {e}")
            continue

        for record in records:
            # Chaque record est sous la forme : [[url], [h1], [texte_brut]]
            message = {
                "url": record[0][0],
                "h1": record[1][0],
                "text": record[2][0]
            }
            try:
                channel.basic_publish(
                    exchange='',
                    routing_key=VECTORIZATION_QUEUE,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logging.info(f"Message envoyé pour {message['url']}")
            except Exception as e:
                logging.error(f"Erreur lors de l'envoi du message pour {message['url']}: {e}")

    connection.close()

if __name__ == "__main__":
    main()
