import faiss
import numpy as np


def index_embeddings(embeddings):
    embeddings = embeddings.astype(np.float32)  # S'assurer que c'est du float32

    d = embeddings.shape[1]
    faiss.omp_set_num_threads(1)
    index = faiss.IndexHNSWFlat(d, 32)

    index.add(embeddings)  # Ajouter les vecteurs à l'index
    faiss.write_index(index, "faiss.index")  # Sauvegarde
