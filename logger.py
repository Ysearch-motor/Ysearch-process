import json
import time
from config import RABBITMQ_USER, RABBITMQ_PASSWORD
import paho.mqtt.client as mqtt

BROKER = '158.69.54.81'
PORT   = 1883
TOPIC  = 'logger'

def logger(data: any):
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
