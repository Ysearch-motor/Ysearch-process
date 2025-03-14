import data_searcher
import sequencer
import vectoriser
import indexer
import numpy as np
import ast

# lire la data depuis le fichier
# file_path = "data.txt"
# with open(file_path, "r", encoding="utf-8") as f:
#     content = f.read()
# textes = ast.literal_eval(content)


def main():
    embeddings = []
    urls = []
    h1s = []
    textes = data_searcher.get_data("fichier.warc.gz")
    for text in textes:
        text_segments = sequencer.segment_text(text[2][0], 150, 2)
        embedding = vectoriser.vectorize_text(text_segments)
        embeddings.append(embedding)
        urls.append(text[0][0])
        h1s.append(text[1][0])
    np.save("urls.npy", urls)
    np.save("h1s.npy", h1s)
    np.save("embeddings.npy", np.array(embeddings))
    indexer.index_embeddings(np.array(embeddings))


if __name__ == "__main__":
    main()
