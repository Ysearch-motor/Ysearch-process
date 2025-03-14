import faiss
from sentence_transformers import SentenceTransformer
import numpy as np

index = faiss.read_index("faiss.index")
urls = np.load("urls.npy")
h1 = np.load("h1s.npy")
model = SentenceTransformer("all-MiniLM-L6-v2")


def search(query: str, k: int = 10):
    query_vector = model.encode([query]).astype("float32")
    D, I = index.search(query_vector, k)
    results = [
        {"url": urls[i], "h1": h1[i], "score": float(D[0][j])}
        for j, i in enumerate(I[0])
    ]
    return {"Résultat": results}


print(search("Politique", 1))
