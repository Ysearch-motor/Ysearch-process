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


# fichier warc à indexer
warc_file = "fichier.warc.gz"


def main():
    # init lists, they will be saved in files
    embeddings = []
    urls = []
    h1s = []

    # get data from the warc file
    textes = data_searcher.get_data(warc_file)

    # segment, vectorize the data
    for text in textes:
        text_segments = sequencer.segment_text(text[2][0], 150, 2)
        embedding = vectoriser.vectorize_text(text_segments)
        embeddings.append(embedding)
        urls.append(text[0][0])
        h1s.append(text[1][0])

    # save the data
    np.save("urls.npy", urls)
    np.save("h1s.npy", h1s)
    np.save("embeddings.npy", np.array(embeddings))

    # index the embeddings
    indexer.index_embeddings(np.array(embeddings))


if __name__ == "__main__":
    main()
