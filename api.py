from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import config

# Initialisation du client Elasticsearch à partir de la configuration
es = Elasticsearch(hosts=config.ES_HOSTS)

# Chargement du modèle d'encodage
model = SentenceTransformer("all-MiniLM-L6-v2")

def search(query: str, k: int = 10):
    # Encode la requête en vecteur et convertit le résultat en liste de float
    query_vector = model.encode([query]).astype("float32").tolist()[0]
    
    # Construction de la requête Elasticsearch utilisant script_score pour calculer la similarité cosinus
    body = {
        "size": k,
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }
    }
    
    # Exécute la recherche sur l'index défini dans config.py
    res = es.search(index=config.ES_INDEX, body=body)
    hits = res["hits"]["hits"]
    
    results = []
    for hit in hits:
        source = hit["_source"]
        results.append({
            "url": source.get("url", ""),
            "h1": source.get("h1", ""),
            "score": hit.get("_score", 0)
        })
    return {"Résultat": results}

print(search("Politique Française",30))

