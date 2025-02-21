import faiss
import numpy as np

# Charger les embeddings
embeddings = np.load("embeddings.npy")
urls = np.load("urls.npy")

# Définir la dimension
d = embeddings.shape[1]

# Créer l'index (avec HNSW pour la vitesse)
index = faiss.IndexHNSWFlat(d, 32)
index.add(embeddings)

# Sauvegarde de l'index
faiss.write_index(index, "faiss.index")
