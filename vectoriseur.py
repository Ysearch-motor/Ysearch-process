from sentence_transformers import SentenceTransformer
import numpy as np
import duckdb

con = duckdb.connect("commoncrawl.db")

# Chargement du modèle d'embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")  # Petit et rapide

# Récupération des textes depuis DuckDB
texts = con.execute("SELECT url, text FROM pages").fetchall()

# Générer les embeddings
embeddings = []
urls = []
for url, text in texts:
    embedding = model.encode(text)  # Convertir en vecteur
    embeddings.append(embedding)
    urls.append(url)

# Convertir en tableau numpy
embeddings = np.array(embeddings)

# Sauvegarde des embeddings et URLs
np.save("embeddings.npy", embeddings)
np.save("urls.npy", urls)
