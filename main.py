import data_searcher
import sequencer
import vectoriser
import indexer
import numpy as np
import ast
import tqdm
import os

# lire la data depuis le fichier

f=open("path.paths", "r")
paths = f.readlines()
f.close()
print(paths)
warc_file = "./warc/fichier.warc.gz"


def main():
    # init lists, they will be saved in files
    for warcurl in tqdm.tqdm(paths):
        warcurl = warcurl.strip()
        embeddings = []
        urls = []
        h1s = []

        # get data from the warc file
        textes = data_searcher.get_data(warcurl, warc_file)

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
        os.system("rm ./warc/fichier.warc.gz")


if __name__ == "__main__":
    main()
