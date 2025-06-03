import json
import time
import threading
import pika
import torch
import logging
import torch.nn.functional as F
import os

from queue import Queue, Empty
from numba import njit
import numpy as np

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

from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.status import Status

TORCH_USE_CUDA_DSA = (
    True  # Utilisation de CUDA DSA pour éviter les copies CPU-GPU inutiles
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

console = Console()
# -------------------------------------------------------------------
# 1) Configuration GPU / modèle
# -------------------------------------------------------------------
# Force CUDA device visibility
os.environ["CUDA_VISIBLE_DEVICES"] = "0"  # Force l'utilisation du premier GPU
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"  # Assure un ordre cohérent des GPUs

# Configurer PyTorch pour utiliser TF32 (améliore les performances sur ampere et plus récent)
torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True
torch.backends.cudnn.benchmark = True  # Auto-tuning des algorithmes

# Assurer que CUDA est initialisé avant de vérifier sa disponibilité
try:
    torch.cuda.init()
except:
    pass  # Déjà initialisé ou erreur d'initialisation

# Vérifier la disponibilité de CUDA
if torch.cuda.is_available():
    # Afficher les informations CUDA de manière détaillée
    logging.info(f"CUDA disponible: {torch.cuda.is_available()}")
    logging.info(f"Nombre de GPUs CUDA: {torch.cuda.device_count()}")
    for i in range(torch.cuda.device_count()):
        logging.info(f"GPU {i}: {torch.cuda.get_device_name(i)}")
    logging.info(f"GPU actif: {torch.cuda.current_device()}")
    logging.info(f"Version PyTorch: {torch.__version__}")
    logging.info(f"Version CUDA: {torch.version.cuda}")

    # Nettoyer le cache GPU
    torch.cuda.empty_cache()

    # Forcer l'utilisation du GPU
    device = torch.device("cuda:0")
    # Vérifier si le GPU est réellement accessible
    test_tensor = torch.tensor([1.0], device=device)
    logging.info(f"Test tensor device: {test_tensor.device}")
    del test_tensor  # Libérer la mémoire
else:
    logging.warning("CUDA n'est pas disponible. Utilisation du CPU.")
    logging.warning(
        "Vérifiez vos pilotes NVIDIA et l'installation de PyTorch avec CUDA."
    )
    device = torch.device("cpu")

logging.info(f"Using device: {device}")

# Initialisation du modèle SentenceTransformer sur GPU avec configuration explicite
model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

# -------------------------------------------------------------------
# 2) Paramètres de batch
# -------------------------------------------------------------------
DOC_BATCH_SIZE = 1000  # max doc messages à traiter par cycle
EMBED_BATCH_SIZE = 512  # taille de batch pour model.encode

# Temps de connexion RabbitMQ (mesuré une seule fois)
time_get_rabbit_connection = 0

# Queue interne thread-safe pour bufferer les messages RabbitMQ
message_queue = Queue()


# -------------------------------------------------------------------
# 3) Fonction Numba pour moyenne + normalisation sur CPU
# -------------------------------------------------------------------
@njit
def compute_doc_embeddings_numba(
    flat_embeddings: np.ndarray, counts: np.ndarray
) -> np.ndarray:
    """
    flat_embeddings: shape (total_segments, dim)
    counts: shape (n_docs,), dtype=int32
    Retourne un tableau (n_docs, dim) contenant pour chaque doc:
      (1) moyenne des segments
      (2) normalisation L2 du résultat
    """
    n_docs = counts.shape[0]
    dim = flat_embeddings.shape[1]
    result = np.zeros((n_docs, dim), dtype=np.float32)

    pos = 0
    for i in range(n_docs):
        c = counts[i]  # nb de segments pour le doc i
        # 1) somme sur c lignes
        for d in range(dim):
            s = 0.0
            # on accumule la composante d pour chaque segment du doc
            for j in range(pos, pos + c):
                s += flat_embeddings[j, d]
            # on stocke la moyenne pour dimension d
            result[i, d] = s / c
        pos += c

        # 2) normalisation L2 du vecteur i
        norm_val = 0.0
        for d in range(dim):
            norm_val += result[i, d] * result[i, d]
        norm_val = np.sqrt(norm_val)
        if norm_val > 0.0:
            for d in range(dim):
                result[i, d] /= norm_val

    return result


# -------------------------------------------------------------------
# 4) Fonction pour établir la connexion RabbitMQ
# -------------------------------------------------------------------
def get_rabbit_connection():
    global time_get_rabbit_connection
    start_time = time.time()
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            # Connection parameters optimized for stability
            connection_params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=30,  # Reduced heartbeat interval (30 seconds)
                socket_timeout=15.0,  # Increased socket timeout (15 seconds)
                blocked_connection_timeout=300.0,  # 5 minute timeout for blocked connections
                retry_delay=RABBITMQ_RETRY_DELAY,
                connection_attempts=3,  # Try to connect 3 times before failing
            )
            
            # Enhanced TCP keepalive options
            connection_params.tcp_options = {
                'TCP_KEEPIDLE': 30,  # Start sending keepalive probes after 30 seconds
                'TCP_KEEPINTVL': 5,   # Send keepalive probes every 5 seconds
                'TCP_KEEPCNT': 10,    # Drop connection after 10 failed probes
                'TCP_USER_TIMEOUT': 30000,  # 30 seconds timeout for TCP operations
            }
            
            # Note: confirm_delivery n'est pas un attribut de ConnectionParameters
            # mais une option pour le canal, on le configurera plus tard si nécessaire
            
            # Create the connection
            connection = pika.BlockingConnection(connection_params)
            
            # Check connection is really established
            if not connection.is_open:
                raise ConnectionError("Connection created but not open")
                
            logging.info("Connected to RabbitMQ")
            time_get_rabbit_connection = time.time() - start_time
            return connection
        except Exception as e:
            logging.error(
                f"RabbitMQ connection error: {e}. Retrying in {RABBITMQ_RETRY_DELAY}s."
            )
            time.sleep(RABBITMQ_RETRY_DELAY)


# -------------------------------------------------------------------
# 5) Thread de traitement en batch
# -------------------------------------------------------------------
class BatchProcessor(threading.Thread):
    """
    Thread dédié à la prise en charge des messages collectés dans message_queue.
    Dès qu'on accumule DOC_BATCH_SIZE messages, ou après un timeout court, on traite le batch.
    Le traitement = segmentation, encodage GPU, passage en Numba pour moy=norm, publication, ACK + log.
    """

    def __init__(self, channel):
        super().__init__(daemon=True)
        self.channel = channel
        self.connection = channel.connection
        self.stop_event = threading.Event()
        self.reconnect_event = threading.Event()
        self.pending_acks = []  # Store methods that need to be ACKed

    def stop(self):
        self.stop_event.set()
    
    def reconnect(self):
        """Reconnect to RabbitMQ if the connection is broken"""
        if self.reconnect_event.is_set():
            return  # Already reconnecting
        
        self.reconnect_event.set()
        try:
            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except:
                    pass  # Already closed or error closing
            
            logging.info("Reconnecting to RabbitMQ...")
            # Reset any pending operations
            time.sleep(0.5)  # Short pause to allow socket cleanup
            
            # Create a fresh connection
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    self.connection = get_rabbit_connection()
                    self.channel = self.connection.channel()
                    self.channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
                    self.channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
                    self.channel.basic_qos(prefetch_count=DOC_BATCH_SIZE)
                    logging.info("Successfully reconnected to RabbitMQ")
                    break
                except Exception as e:
                    if attempt < max_attempts - 1:
                        sleep_time = (attempt + 1) * 2
                        logging.error(f"Reconnection attempt {attempt+1} failed: {e}. Retrying in {sleep_time}s...")
                        time.sleep(sleep_time)
                    else:
                        logging.error(f"Failed to reconnect after {max_attempts} attempts: {e}")
                        raise
        except Exception as e:
            logging.error(f"Critical error during reconnection: {e}")
        finally:
            self.reconnect_event.clear()

    def safe_publish(self, exchange, routing_key, body, properties):
        """Safely publish a message with reconnection attempts"""
        max_retries = 5  # Increased retries
        for attempt in range(max_retries):
            try:
                # Créer une nouvelle connexion si nécessaire (au lieu de reconnecter)
                if not self.connection or not self.connection.is_open:
                    # Forcer la création d'une nouvelle connexion pour éviter les bugs de Pika
                    logging.warning("Connection not open, creating new connection")
                    try:
                        if self.connection:
                            try:
                                self.connection.close()
                            except:
                                pass
                        self.connection = get_rabbit_connection()
                    except Exception as e:
                        logging.error(f"Failed to create new connection: {e}")
                        if attempt < max_retries - 1:
                            sleep_time = (attempt + 1) * 1.0
                            time.sleep(sleep_time)
                            continue
                        else:
                            return False
                
                # Toujours recréer un canal frais pour chaque tentative afin d'éviter l'erreur "pop from empty deque"
                try:
                    # Fermer le canal existant s'il existe
                    if self.channel and self.channel.is_open:
                        try:
                            self.channel.close()
                        except:
                            pass
                    
                    # Créer un nouveau canal
                    self.channel = self.connection.channel()
                    self.channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
                    self.channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
                    self.channel.basic_qos(prefetch_count=DOC_BATCH_SIZE)
                except Exception as e:
                    logging.error(f"Error creating channel: {e}")
                    if attempt < max_retries - 1:
                        sleep_time = (attempt + 1) * 0.5
                        time.sleep(sleep_time)
                        continue
                    else:
                        return False
                
                # Adapter la stratégie en fonction de la taille du message
                body_data = body
                msg_size = len(body_data)
                
                # Pour les messages volumineux, utiliser des propriétés optimisées
                if msg_size > 1000000:  # 1MB
                    logging.warning(f"Large message detected ({msg_size/1000000:.2f}MB)")
                    # Configuration optimisée pour les gros messages
                    props = pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                        content_type='application/json',
                        expiration='86400000',  # 24h TTL
                    )
                else:
                    props = properties
                
                # Traiter les événements en attente avant publication pour éviter les conditions de course
                try:
                    self.connection.process_data_events(0)  # 0 = non-blocking
                except:
                    # Ignorer les erreurs ici, car process_data_events peut lever des exceptions 
                    # si la connexion est fermée
                    pass
                
                # Publication 
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body_data,
                    properties=props
                )
                
                # Traiter les événements encore une fois pour assurer la transmission
                try:
                    self.connection.process_data_events(0)
                except:
                    pass
                
                return True
            except (pika.exceptions.AMQPError, pika.exceptions.StreamLostError, 
                   AssertionError, ConnectionError, IndexError) as e:
                # Ajout de IndexError pour capturer explicitement l'erreur "pop from an empty deque"
                logging.error(f"Error publishing message (attempt {attempt+1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Pour cet type d'erreur spécifique, forcer un délai plus long
                    if isinstance(e, IndexError) and "pop from an empty deque" in str(e):
                        logging.warning("Detected 'pop from an empty deque' error, forcing longer delay and full reconnection")
                        sleep_time = 2.0 * (attempt + 1)  # Délai plus long pour ce type d'erreur
                    else:
                        sleep_time = (attempt + 1) * 0.5
                    
                    # Recréer complètement la connexion après une erreur
                    try:
                        if self.connection:
                            try:
                                self.connection.close()
                            except:
                                pass
                        time.sleep(0.5)  # Pause pour permettre au socket de se fermer complètement
                        self.connection = get_rabbit_connection()
                    except Exception as conn_err:
                        logging.error(f"Reconnection error: {conn_err}")
                    
                    logging.info(f"Waiting {sleep_time}s before retry...")
                    time.sleep(sleep_time)
                else:
                    return False
            except Exception as e:
                logging.error(f"Unexpected error publishing message: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1.0)
                else:
                    return False
    
    def safe_ack(self, delivery_tag):
        """Safely acknowledge a message with reconnection attempts"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not self.connection.is_open:
                    self.reconnect()
                
                self.channel.basic_ack(delivery_tag=delivery_tag)
                return True
            except (pika.exceptions.AMQPError, pika.exceptions.StreamLostError, 
                   AssertionError, ConnectionError) as e:
                logging.error(f"Error acknowledging message (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    try:
                        self.reconnect()
                    except Exception as conn_err:
                        logging.error(f"Reconnection error during ACK: {conn_err}")
                    
                    # Adaptive backoff - shorter than for publishing
                    sleep_time = 0.2 * (attempt + 1)
                    time.sleep(sleep_time)
                else:
                    return False
            except Exception as e:
                logging.error(f"Unexpected error acknowledging message: {e}")
                return False

    def run(self):
        global time_get_rabbit_connection

        # On pré-calcule la dimension d'embedding pour initialiser le tableau numpy plus tard
        dummy = model.encode(
            [""],
            batch_size=1,
            convert_to_tensor=True,
            show_progress_bar=False,
            device=device,
        )
        # Pas besoin de stocker la dimension d'embedding car compute_doc_embeddings_numba l'extrait automatiquement
        del dummy  # on n'en a plus besoin

        while not self.stop_event.is_set():
            try:
                batch = []
                # 5.1) Récupérer jusqu'à DOC_BATCH_SIZE items ou timeout 0.1s
                start_wait = time.time()
                while len(batch) < DOC_BATCH_SIZE and (time.time() - start_wait) < 0.1:
                    try:
                        method, body = message_queue.get(timeout=0.01)
                        batch.append((method, body))
                    except Empty:
                        pass

                if not batch:
                    # Petite pause pour ne pas boucler à 100% CPU
                    time.sleep(0.05)
                    continue

                # -----------------------------------------------------------
                # 5.2) Phase de parsing + segmentation (CPU)
                # -----------------------------------------------------------
                t0 = time.time()
                all_segments = []  # liste de toutes les fenêtres de texte
                counts = []  # nombres de segments par document
                docs = []  # liste de (method, message_json) pour republishing + ack + log

                # Barre de progression pour la segmentation
                with Progress(
                    "[progress.description]{task.description}",
                    BarColumn(bar_width=None),
                    "[progress.percentage]{task.percentage:>3.0f}%",
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    console=console,
                ) as progress:
                    task_seg = progress.add_task("Segmentation des docs", total=len(batch))
                    for method, body in batch:
                        message = json.loads(body)
                        segments = segment_text(message["text"], 150, 2)
                        counts.append(len(segments))
                        all_segments.extend(segments)
                        docs.append((method, message))
                        progress.update(task_seg, advance=1)
                time_encode = time.time() - t0

                # -----------------------------------------------------------
                # 5.3) Encodage GPU en sous-batches
                # -----------------------------------------------------------
                t1 = time.time()
                with Status("[bold green]Encodage GPU en cours...", console=console):
                    # model.encode renvoie un tensor (total_segments, dim_embedding) sur GPU
                    embeddings_tensor = model.encode(
                        all_segments,
                        batch_size=EMBED_BATCH_SIZE,
                        convert_to_tensor=True,
                        show_progress_bar=False,
                        device=device,
                    )
                time_embeding = time.time() - t1

                # -----------------------------------------------------------
                # 5.4) Passage sur CPU + transformation en numpy 2D
                # -----------------------------------------------------------
                embeddings_cpu = (
                    embeddings_tensor.cpu().numpy()
                )  # shape = (total_segments, dim_embedding)
                counts_np = np.array(counts, dtype=np.int32)

                # -----------------------------------------------------------
                # 5.5) Appel à la fonction Numba pour moyennes + normalisation
                # -----------------------------------------------------------
                with Status(
                    "[bold blue]Calcul Numba (moyenne + norm) en cours...", console=console
                ):
                    doc_embeddings_np: np.ndarray = compute_doc_embeddings_numba(
                        embeddings_cpu, counts_np
                    )
                # doc_embeddings_np.shape == (n_docs, dim_embedding)

                # -----------------------------------------------------------
                # 5.6) Publication, ACK & logging (un log par document)
                # -----------------------------------------------------------
                with Progress(
                    "[progress.description]{task.description}",
                    BarColumn(bar_width=None),
                    "[progress.percentage]{task.percentage:>3.0f}%",
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    console=console,
                ) as progress_pub:
                    task_pub = progress_pub.add_task("Publication & ACK", total=len(docs))
                    
                    # Verify connection is active before starting publication
                    if not self.connection.is_open:
                        self.reconnect()
                    
                    # Traiter les événements de connexion avant de commencer la publication
                    try:
                        self.connection.process_data_events()
                    except (pika.exceptions.AMQPError, AssertionError, ConnectionError, IndexError):
                        logging.warning("Connection error (ou IndexError) avant publication, reconnexion en cours…")
                        self.reconnect()
                    
                    for idx_doc, ((method, message), emb_np) in enumerate(
                        zip(docs, doc_embeddings_np)
                    ):
                        new_msg = {
                            "url": message["url"],
                            "h1": message["h1"],
                            "embedding": emb_np.tolist(),
                        }
                        
                        # Vérifier périodiquement l'état de la connexion pour les lots importants
                        if idx_doc > 0 and idx_doc % 50 == 0:
                            try:
                                self.connection.process_data_events()
                            except (pika.exceptions.AMQPError, AssertionError, ConnectionError, IndexError):
                                logging.warning(f"Connection error (ou IndexError) détecté durant la publication à l’item {idx_doc}, reconnexion…")
                                self.reconnect()
                        
                        # Publication (persistent) with safe wrapper
                        published = self.safe_publish(
                            exchange="",
                            routing_key=INDEXING_QUEUE,
                            body=json.dumps(new_msg),
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                        
                        # ACK du message d'origine with safe wrapper
                        if published:
                            acked = self.safe_ack(delivery_tag=method.delivery_tag)
                            if not acked:
                                logging.warning(f"Failed to ACK message for URL: {message['url']}")
                                self.pending_acks.append(method.delivery_tag)
                        else:
                            logging.error(f"Failed to publish message for URL: {message['url']}")
                            # Store for potential retry later
                            self.pending_acks.append(method.delivery_tag)

                        # Logging personnalisé
                        log_data = {
                            "step": "vector",
                            "url": message["url"],
                            "time_encode": time_encode,
                            "time_embeding": time_embeding,
                            "time_get_rabbit_connection": time_get_rabbit_connection,
                            "computer": MACHINE,
                        }
                        try:
                            logger(log_data)
                        except Exception as e:
                            logging.error(f"Error logging data: {e}")

                        progress_pub.update(task_pub, advance=1)

                # Un petit retour pour savoir qu'on a traité un batch complet
                logging.info(
                    f"Batch traité: {len(batch)} docs ⇢ encode_cpu={time_encode:.3f}s, "
                    f"gpu_embed={time_embeding:.3f}s, "
                    f"reduction_numba={(time.time() - t1 - time_embeding):.3f}s"
                )
                
                # Retry any pending ACKs
                if self.pending_acks and self.connection.is_open:
                    for tag in list(self.pending_acks):
                        if self.safe_ack(tag):
                            self.pending_acks.remove(tag)
            
            except Exception as e:
                logging.error(f"Error in batch processing: {e}")
                # Sleep briefly before continuing to next batch
                time.sleep(1)


# -------------------------------------------------------------------
# 6) Callback RabbitMQ : on_message
# -------------------------------------------------------------------
def on_message(channel, method, properties, body):
    """
    RabbitMQ appelle cette fonction à chaque nouveau message.
    On stocke simplement le (method, body) dans la Queue interne.
    """
    message_queue.put((method, body))


# -------------------------------------------------------------------
# 7) Fonction principale
# -------------------------------------------------------------------
def main():
    # 7.1) Connexion RabbitMQ
    connection = None
    processor = None
    try:
        connection = get_rabbit_connection()
        channel = connection.channel()
        logging.info(f"Created channel={channel.channel_number}")
        channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
        channel.queue_declare(queue=INDEXING_QUEUE, durable=True)

        # 7.2) QoS pour préfetch + batch
        channel.basic_qos(prefetch_count=DOC_BATCH_SIZE)

        # 7.3) Démarrage du thread de traitement
        processor = BatchProcessor(channel)
        processor.start()

        # 7.4) Enregistrement du callback et start_consuming
        channel.basic_consume(
            queue=VECTORIZATION_QUEUE, on_message_callback=on_message, auto_ack=False
        )

        logging.info("Batch Vectorizer Consumer awaiting messages (mode push + Numba) ...")
        
        # Boucle de reconnexion automatique
        while True:
            try:
                # Vérifier que le canal est toujours actif
                if not connection.is_open or not channel.is_open:
                    raise pika.exceptions.AMQPConnectionError("Connection or channel closed")
                
                # Démarrer la consommation avec surveillance
                channel.start_consuming()
                
            except KeyboardInterrupt:
                logging.info("Interruption manuelle reçue, arrêt du consumer.")
                break
            except (pika.exceptions.ConnectionClosedByBroker, 
                   pika.exceptions.AMQPChannelError,
                   pika.exceptions.AMQPConnectionError,
                   AssertionError) as e:
                # Reconnexion automatique en cas d'erreur RabbitMQ
                logging.warning(f"Connection error: {e}, attempting to reconnect...")
                
                # Attendre un peu avant de reconnecter
                time.sleep(2)
                
                try:
                    # Recréer la connexion et le canal
                    if connection and connection.is_open:
                        try:
                            connection.close()
                        except:
                            pass
                    
                    connection = get_rabbit_connection()
                    channel = connection.channel()
                    channel.queue_declare(queue=VECTORIZATION_QUEUE, durable=True)
                    channel.queue_declare(queue=INDEXING_QUEUE, durable=True)
                    channel.basic_qos(prefetch_count=DOC_BATCH_SIZE)
                    
                    # Réenregistrer le callback
                    channel.basic_consume(
                        queue=VECTORIZATION_QUEUE, on_message_callback=on_message, auto_ack=False
                    )
                    
                    # Notifier le thread de processing de la nouvelle connexion
                    if processor:
                        processor.connection = connection
                        processor.channel = channel
                    
                    logging.info("Successfully reconnected, resuming operations...")
                    continue
                except Exception as reconnect_err:
                    logging.error(f"Failed to reconnect: {reconnect_err}")
                    time.sleep(5)  # Attendre plus longtemps avant de réessayer
                    continue
            except Exception as e:
                logging.error(f"Erreur dans le consumer principal: {e}")
                time.sleep(2)
                continue
    except KeyboardInterrupt:
        logging.info("Interruption du programme, arrêt propre...")
        if processor:
            processor.stop()
        if connection and connection.is_open:
            try:
                connection.close()
            except:
                pass
    except Exception as e:
        logging.error(f"Erreur critique dans la fonction principale: {e}")
    
    return 0  # Return zero for normal exit


if __name__ == "__main__":
    exit_code = main()
    import sys
    sys.exit(exit_code)
