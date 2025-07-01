import json
import time
import pika
import torch
import logging
import torch.nn.functional as F
from sequencer import segment_text
from sentence_transformers import SentenceTransformer
from logger import logger
from config import (
    RABBITMQ_HOST,
    VECTORIZATION_QUEUE,
    INDEXING_QUEUE,
    RABBITMQ_RETRY_DELAY,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    MACHINE,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Device configuration (GPU if available)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logging.info(f"Using device: {device}")

# Initialize the SentenceTransformer model on the GPU
model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

time_encode = 0
time_embeding=0
time_get_rabbit_connection = 0

# Batch sizes
DOC_BATCH_SIZE = 10000        # Number of documents to pull per RabbitMQ batch
EMBED_BATCH_SIZE = 512        # Number of segments per GPU encode batch

def get_rabbit_connection() -> pika.BlockingConnection:
    """Ouvre une connexion RabbitMQ et mesure le temps nécessaire."""
    global time_get_rabbit_connection
    start_time = time.time()
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            logging.info("Connected to RabbitMQ")
            time_get_rabbit_connection = time.time() - start_time
            return connection
        except Exception as e:
            logging.error(
                f"RabbitMQ connection error: {e}. Retrying in {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)


def process_batch(channel: pika.adapters.blocking_connection.BlockingChannel) -> int:
    """Traite un lot de documents et renvoie le nombre d'éléments traités."""
    global time_encode
    global time_embeding
    # 1) Pull a batch of messages
    msgs = []
    for _ in range(DOC_BATCH_SIZE):
        method, properties, body = channel.basic_get(
            queue=VECTORIZATION_QUEUE,
            auto_ack=False
        )
        if method:
            msgs.append((method, body))
        else:
            break

    if not msgs:
        return 0

    start_time = time.time()
    # 2) Parse and segment text
    all_segments = []
    counts = []    # number of segments per doc
    docs = []      # original messages
    for method, body in msgs:
        message = json.loads(body)
        segments = segment_text(message['text'], 150, 2)
        counts.append(len(segments))
        all_segments.extend(segments)
        docs.append((method, message))
    time_encode = time.time() - start_time

    # 3) Encode all segments in batches on GPU
    start_time = time.time()
    embeddings = model.encode(
        all_segments,
        batch_size=EMBED_BATCH_SIZE,
        convert_to_tensor=True,
        show_progress_bar=False,
        device=device
    )
    time_embeding = time.time() - start_time
    # 4) Split embeddings by document and compute normalized mean
    doc_embeddings = []
    idx = 0
    for count in counts:
        chunk = embeddings[idx: idx + count]
        mean_emb = torch.mean(chunk, dim=0)
        norm_emb = F.normalize(mean_emb, p=2, dim=0)
        doc_embeddings.append(norm_emb.cpu().numpy())
        idx += count

    # 5) Publish embeddings and ack messages
    for (method, message), emb in zip(docs, doc_embeddings):
        new_msg = {
            "url": message["url"],
            "h1": message["h1"],
            "embedding": emb.tolist()
        }
        channel.basic_publish(
            exchange='',
            routing_key=INDEXING_QUEUE,
            body=json.dumps(new_msg),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"Processed and acked: {message['url']}")
        data = {
            "step": "vector",
            "url": message["url"],
            "time_encode": time_encode,
            "time_embeding": time_embeding,
            "time_get_rabbit_connection": time_get_rabbit_connection,
            "computer": MACHINE,
        }
        logger(data)

    return len(msgs)


def main() -> None:
    """Boucle principale du consumer vectorisation GPU."""
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=DOC_BATCH_SIZE)
    logging.info("Batch Vectorizer Consumer awaiting messages...")

    try:
        while True:
            processed = process_batch(channel)
            if processed == 0:
                time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Manual interruption, shutting down consumer.")
    except Exception as e:
        logging.error(f"Consumer error: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()