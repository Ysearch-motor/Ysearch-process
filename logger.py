import json
import time
from typing import Any
from config import RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_HOST
import paho.mqtt.client as mqtt

BROKER = RABBITMQ_HOST
PORT   = 1883
TOPIC  = 'logger'

def logger(data: Any) -> None:
    """Publie un dictionnaire de données sur le topic MQTT.

    :param Any data: données sérialisables en JSON
    :return: ``None``
    :rtype: None
    """
    # 1) Création du client MQTT
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(RABBITMQ_USER, RABBITMQ_PASSWORD)

    # 2) Connexion au broker
    client.connect(BROKER, PORT, keepalive=60)

    # 3) Démarrage de la boucle réseau
    client.loop_start()

    # 4) Publication du payload
    payload = json.dumps(data)
    print(f"[PUB] {payload}")
    result = client.publish(TOPIC, payload, qos=1)

    # 5) Option A : bloquer jusqu’à confirmation
    result.wait_for_publish()

    # –– ou Option B : on peut aussi attendre un petit peu
    # time.sleep(0.2)

    # 6) Arrêt de la boucle et déconnexion
    client.loop_stop()
    client.disconnect()
