import pika
import json
import logging
import time
from config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASSWORD, DOWNLOAD_QUEUE, RABBITMQ_RETRY_DELAY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_rabbit_connection() -> pika.BlockingConnection:
    """Ouvre une connexion RabbitMQ.

    Cette fonction tente de se connecter en boucle jusqu'à ce que la
    connexion aboutisse.

    :return: objet de connexion ouvert
    :rtype: pika.BlockingConnection
    """
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
    """Publie les URLs WARC dans RabbitMQ.

    Les chemins sont lus depuis le fichier ``path.paths``.

    :return: ``None``
    :rtype: None
    """
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=DOWNLOAD_QUEUE, durable=True)
    
    # Suppose que vos URLs sont stockées dans un fichier "download_urls.txt"
    # avec une URL par ligne.
    try:
        with open("path.paths", "r") as f:
            urls = f.readlines()
    except Exception as e:
        logging.error(f"Erreur de lecture du fichier download_urls.txt : {e}")
        return
    
    for url in urls:
        url = url.strip()
        message = {"warc_url": url}
        try:
            channel.basic_publish(
                exchange='',
                routing_key=DOWNLOAD_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logging.info(f"URL publiée dans la file de téléchargement: {url}")
        except Exception as e:
            logging.error(f"Erreur lors de l'envoi de l'URL {url}: {e}")
    
    connection.close()

if __name__ == "__main__":
    main()
