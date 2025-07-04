import json
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime, timezone
from config import (
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    RABBITMQ_HOST,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_USER,
    MONGO_PASS,
    MONGO_AUTH_SRC,
)

# === MongoDB ===
uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SRC}"
mongo_client = MongoClient(uri)
db = mongo_client["logger"]

if "warc_logs" not in db.list_collection_names():
    db.create_collection(
        "warc_logs",
        timeseries={
            "timeField": "Created_at",
            "metaField": "warc_url",
            "granularity": "seconds",
        },
    )

if "vector_logs" not in db.list_collection_names():
    db.create_collection(
        "vector_logs",
        timeseries={
            "timeField": "Created_at",
            "metaField": "url",
            "granularity": "seconds",
        },
    )

if "index_logs" not in db.list_collection_names():
    db.create_collection(
        "index_logs",
        timeseries={
            "timeField": "Created_at",
            "metaField": "url",
            "granularity": "seconds",
        },
    )

# === MQTT ===
BROKER = RABBITMQ_HOST
PORT   = 1883
TOPIC  = "logger"

def on_connect(
    client: mqtt.Client,
    userdata: object,
    flags: dict,
    rc: int,
    properties: mqtt.Properties | None = None,
) -> None:
    """Callback MQTT exécuté lors de la connexion.

    :param mqtt.Client client: client MQTT utilisé
    :param object userdata: données utilisateur passées au callback
    :param dict flags: indicateurs de connexion
    :param int rc: code retour du broker
    :param mqtt.Properties properties: propriétés supplémentaires
    :return: ``None``
    :rtype: None
    """
    # rc = 0   → connexion acceptée
    # rc = 4   → bad username/password (ou anonyme interdit)
    print(f"Connected with code {rc}")
    if rc == 0:
        client.subscribe(TOPIC, qos=1)

def on_message(client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
    """Stocke chaque message reçu dans la base MongoDB.

    :param mqtt.Client client: client MQTT utilisé
    :param object userdata: données utilisateur associées
    :param mqtt.MQTTMessage msg: message MQTT reçu
    :return: ``None``
    :rtype: None
    """
    try:
        payload_dict = json.loads(msg.payload.decode())
    except json.JSONDecodeError:
        print(f"Invalid JSON payload: {msg.payload!r}")
        return

    step = payload_dict.pop("step", None)
    if step == "warc":
        collection = db["warc_logs"]
    elif step == "vector":
        collection = db["vector_logs"]
    elif step == "index":
        collection = db["index_logs"]
    else:
        print(f"Unknown step in payload: {step!r}")
        return

    doc = {
        "Created_at": datetime.now(timezone.utc),
        **payload_dict
    }
    collection.insert_one(doc)
    print(f"[RCV] {msg.topic} → {payload_dict} (saved in {collection.name})")

def main() -> None:
    """Démarre l'abonnement MQTT et reste en écoute infinie.

    :return: ``None``
    :rtype: None
    """
    # on passe à l’API v2 pour les callbacks
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(RABBITMQ_USER, RABBITMQ_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_forever()

if __name__ == "__main__":
    main()
