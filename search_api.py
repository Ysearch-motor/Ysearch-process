from fastapi import FastAPI
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

# Charger l'index et les URLs
index = faiss.read_index("faiss.index")
urls = np.load("urls.npy")
model = SentenceTransformer("all-MiniLM-L6-v2")

app = FastAPI()


@app.get("/search")
def search(query: str, k: int = 5):
    query_vector = model.encode([query]).astype("float32")
    D, I = index.search(query_vector, k)  # Recherche les k plus proches
    results = [{"url": urls[i], "score": float(D[0][j])} for j, i in enumerate(I[0])]
    return {"results": results}


# Lancer le serveur avec : uvicorn search_api:app --reload
