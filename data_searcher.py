import sys
import requests
import trafilatura
from bs4 import BeautifulSoup
from langdetect import detect
from warcio.archiveiterator import ArchiveIterator
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_record(record_data):
    """
    Fonction de traitement d'un enregistrement.
    Retourne [url], [h1] et [texte_brut] si le texte est en français.
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

def get_data(warcurl, warc_file):
    """
    Extrait les données d'un fichier WARC en parallèle et retourne une liste
    contenant [url], [h1] et [texte_brut] pour chaque page en français.
    """
    data = []
    records = []

    get_warc(warcurl)

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
            print(f"Traitement de l'enregistrement n°{i}")

    # Sauvegarde des données dans un fichier pour garder une trace
    with open("data.txt", "w", encoding="utf-8") as f:
        for row in data:
            f.write(f"{row},\n")

    return data

def get_warc(warcurl: str):
    """
    Télécharge un fichier WARC à partir d'une URL et le sauvegarde localement.
    """
    response = requests.get("https://data.commoncrawl.org/"+warcurl)
    print("fichier téléchargé avec succès")
    if response.status_code == 200:
        with open("./warc/fichier.warc.gz", "wb") as f:
            f.write(response.content)
            print("fichier écris avec succès")
