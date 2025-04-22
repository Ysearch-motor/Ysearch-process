import json
import time
import pika
import torch
import logging
import numpy as np
import torch.nn.functional as F
from sequencer import segment_text
from sentence_transformers import SentenceTransformer
from config import (
    RABBITMQ_HOST,
    VECTORIZATION_QUEUE,
    INDEXING_QUEUE,
    RABBITMQ_RETRY_DELAY,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure device to GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logging.info(f"Using device: {device}")

# Initialize the SentenceTransformer model on the GPU\model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

def vectorize_text(segments, batch_size=64):
    """
    Vectorizes a list of text segments using a pre-trained model on GPU.

    Args:
        segments (list of str): A list of text segments to be vectorized.
        batch_size (int): Number of segments per batch during encoding.

    Returns:
        numpy.ndarray: A normalized mean embedding ready for indexing.
    """
    # Encode all segments in batches, returning tensors on the device
    embeddings = model.encode(
        segments,
        batch_size=batch_size,
        convert_to_tensor=True,
        show_progress_bar=False,
        device=device
    )

    # Compute mean embedding on GPU
    mean_emb = torch.mean(embeddings, dim=0)
    # Normalize the embedding (L2 norm)
    normalized_emb = F.normalize(mean_emb, p=2, dim=0)
    # Move to CPU and convert to numpy
    return normalized_emb.cpu().numpy()

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
        # Vectorize segments on GPU
        embedding = vectorize_text(segments)
        new_message = {
            "url": message["url"],
            "h1": message["h1"],
            "embedding": embedding.tolist()
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
