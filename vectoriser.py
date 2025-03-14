from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")


def vectorize_text(segments):
    """
    Vectorizes a list of text segments using a pre-trained model.

    Args:
        segments (list of str): A list of text segments to be vectorized.

    Returns:
        list of numpy.ndarray: A list of embeddings corresponding to the input text segments.
    """
    embeddings = []
    for segment in segments:
        embedding = model.encode(segment)
        embeddings.append(embedding)

    mean_embedding = np.mean(embeddings, axis=0)

    normalized_mean_embedding = mean_embedding / np.linalg.norm(mean_embedding)
    return normalized_mean_embedding
