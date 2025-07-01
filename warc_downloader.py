import os
import sys
import pika
import json
import time
import logging
import hashlib
import requests
import trafilatura
import logger as logger
from bs4 import BeautifulSoup
from langdetect import detect
from warcio.archiveiterator import ArchiveIterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Optional, Tuple
from config import (
    RABBITMQ_HOST,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    DOWNLOAD_QUEUE,
    VECTORIZATION_QUEUE,
    RABBITMQ_RETRY_DELAY,
    MACHINE
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
time_thrait = 0
time_download = 0
time_load = 0
time_get_rabbit_connection = 0

def process_record(record_data: Tuple[str, str]) -> Optional[list[list[str]]]:
    """Traite un enregistrement WARC.

    Args:
        record_data (Tuple[str, str]): couple ``(url, html)``.

    Returns:
        Optional[list[list[str]]]: ``[[url], [h1], [texte_brut]]`` ou ``None`` si non français.
    """
    url, html = record_data
    text_brut = trafilatura.extract(html)
    if text_brut:
        try:
            if detect(text_brut) == "fr":
                # Utilisation de lxml pour un parsing plus rapide
                soup = BeautifulSoup(html, "lxml")
                h1 = soup.h1.get_text() if soup.h1 else ""
                return [[url], [h1], [text_brut]]
        except Exception as e:
            sys.stderr.write(f"Skipping record due to error: {e}\n")
    return None


def get_data(warc_file: str) -> List[list[str]]:
    """Extrait toutes les pages françaises d'un fichier WARC.

    Args:
        warc_file (str): chemin local du fichier WARC.

    Returns:
        list[list[str]]: liste des triplets ``[[url], [h1], [texte]]``.
    """
    data = []
    records = []

    # Lecture séquentielle du fichier et collecte des données brutes
    with open(warc_file, "rb") as f:
        for record in ArchiveIterator(f):
            if record.rec_type == "response":
                url = record.rec_headers.get_header("WARC-Target-URI")
                html = record.content_stream().read().decode(errors="ignore")
                records.append((url, html))

    # Traitement parallèle avec ProcessPoolExecutor
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_record, rec) for rec in records]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                data.append(result)
            # print(f"Traitement de l'enregistrement n°{i}")

    # Sauvegarde des données dans un fichier pour garder une trace
    with open("data.txt", "w", encoding="utf-8") as f:
        for row in data:
            f.write(f"{row},\n")

    return data


def get_rabbit_connection() -> pika.BlockingConnection:
    """Ouvre une connexion RabbitMQ avec heartbeat prolongé."""
    global time_get_rabbit_connection  # track connection time
    while True:
        try:
            time_start = time.time()
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=600,  # augmenter le heartbeat
                blocked_connection_timeout=300,  # délai de blocage
            )
            connection = pika.BlockingConnection(parameters)
            logging.info("Connecté à RabbitMQ")
            time_get_rabbit_connection = time.time() - time_start
            return connection
        except Exception as e:
            logging.error(
                f"Erreur de connexion à RabbitMQ : {e}. Nouvelle tentative dans {RABBITMQ_RETRY_DELAY} secondes."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)


def download_warc(warc_url: str, local_file: str) -> bool:
    """Télécharge un fichier WARC à l'emplacement indiqué."""
    global time_download  # track download time
    start_download = time.time()
    try:
        url = "https://data.commoncrawl.org/" + warc_url
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(local_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            time_download = time.time() - start_download
            logging.info(f"WARC téléchargé: {local_file} en {time_download:.2f}s")
            return True
        else:
            logging.error(
                f"Échec du téléchargement pour {url}, status: {response.status_code}"
            )
            return False
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement de {url}: {e}")
        return False


def callback(ch, method, properties, body) -> None:  # noqa: C901
    """Traite un message contenant une URL WARC."""
    global time_load, time_thrait, time_download, time_get_rabbit_connection  # track load and processing times
    try:
        message = json.loads(body)
        warc_url = message["warc_url"]
        # Générer un nom de fichier unique à partir de l'URL pour éviter les collisions
        file_hash = hashlib.md5(warc_url.encode()).hexdigest()
        local_file = f"./warc/{file_hash}.warc.gz"

        # Télécharger le fichier WARC
        if not download_warc(warc_url, local_file):
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # Charger et mesurer le temps de chargement des données
        start_load = time.time()
        records = get_data(local_file)
        time_load = time.time() - start_load
        logging.info(f"Données chargées en {time_load:.2f}s")
        # Démarrer le chronomètre de traitement
        start_trait = time.time()

        # Créer une connexion dédiée pour la publication
        try:
            publisher_connection = get_rabbit_connection()
            publisher_channel = publisher_connection.channel()
            publisher_channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
        except Exception as pub_e:
            logging.error(
                f"Erreur lors de la création de la connexion de publication: {pub_e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # Pour chaque record, tenter de publier en gérant les erreurs BrokenPipeError
        for record in records:
            out_message = {
                "url": record[0][0],
                "h1": record[1][0],
                "text": record[2][0],
            }
            published = False
            retry_count = 0
            while not published and retry_count < 3:
                try:
                    publisher_channel.basic_publish(
                        exchange="",
                        routing_key=VECTORIZATION_QUEUE,
                        body=json.dumps(out_message),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    published = True
                    # logging.info(f"Message envoyé pour {out_message['url']}")
                except BrokenPipeError as bpe:
                    logging.error(
                        f"BrokenPipeError lors de l'envoi du message pour {out_message['url']}: {bpe}"
                    )
                    retry_count += 1
                    time.sleep(2)
                    try:
                        publisher_channel.close()
                    except Exception:
                        pass
                    try:
                        publisher_connection.close()
                    except Exception:
                        pass
                    # Recréer la connexion de publication
                    try:
                        publisher_connection = get_rabbit_connection()
                        publisher_channel = publisher_connection.channel()
                        publisher_channel.queue_declare(
                            queue=VECTORIZATION_QUEUE, durable=True
                        )
                    except Exception as recon_e:
                        logging.error(f"Erreur lors de la reconnexion: {recon_e}")
                except Exception as e:
                    logging.error(
                        f"Erreur lors de l'envoi du message pour {out_message['url']}: {e}"
                    )
                    retry_count += 1
                    time.sleep(2)
            if not published:
                logging.error(
                    f"Échec de l'envoi du message pour {out_message['url']} après plusieurs tentatives."
                )
                try:
                    publisher_channel.close()
                    publisher_connection.close()
                except Exception:
                    pass
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

        # Fermer la connexion de publication
        try:
            publisher_channel.close()
            publisher_connection.close()
        except Exception as e:
            logging.error(
                f"Erreur lors de la fermeture de la connexion de publication: {e}"
            )

        # Supprimer le fichier téléchargé pour libérer de l'espace
        try:
            os.remove(local_file)
            logging.info(f"Fichier supprimé: {local_file}")
        except Exception as e:
            logging.error(f"Erreur lors de la suppression de {local_file}: {e}")

        # Mesurer le temps de traitement et logger tous les temps
        time_thrait = time.time() - start_trait
        data = {
            "step": "warc",
            "warc_url": warc_url,
            "total_time": time_thrait+time_load+time_get_rabbit_connection+time_download,
            "download_time": time_download,
            "load_time": time_load,
            "processing_time": time_thrait,
            "rabbit_connection_time": time_get_rabbit_connection,
            "computer":MACHINE
        }
        logger.logger(data)
        # logging.info(
        #     f"Temps total - Connexion RabbitMQ: {time_get_rabbit_connection:.2f}s, Download: {time_download:.2f}s, Load: {time_load:.2f}s, Traitement: {time_thrait:.2f}s"
        # )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(
            f"Erreur dans le callback du downloader pour le message {body}: {e}"
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main() -> None:
    """Démarre le consumer de téléchargement WARC."""
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=DOWNLOAD_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DOWNLOAD_QUEUE, on_message_callback=callback)
    logging.info("WARC Downloader en attente de messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interruption manuelle, arrêt du downloader.")
        try:
            if connection and connection.is_open:
                connection.close()
        except Exception as e:
            logging.error(f"Erreur lors de la fermeture de la connexion: {e}")
    except Exception as e:
        logging.error(f"Erreur dans le downloader: {e}")
    finally:
        try:
            if connection and connection.is_open:
                connection.close()
        except Exception as e:
            logging.error(
                f"Erreur lors de la fermeture de la connexion dans le finally: {e}"
            )


if __name__ == "__main__":
    main()
