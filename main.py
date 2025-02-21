from warcio.archiveiterator import ArchiveIterator
import trafilatura
import duckdb

# Fichier WARC à lire
warc_file = "fichier.warc.gz"

# Stocker les données dans DuckDB
con = duckdb.connect("commoncrawl.db")
con.execute("CREATE TABLE IF NOT EXISTS pages (url TEXT, text TEXT)")

i = 0
with open(warc_file, "rb") as f:
    for record in ArchiveIterator(f):
        print(i)
        if record.rec_type == "response":
            url = record.rec_headers.get_header("WARC-Target-URI")
            html = record.content_stream().read().decode(errors="ignore")
            text = trafilatura.extract(html)  # Extraction propre du texte

            if text:  # Sauvegarde dans DuckDB
                con.execute("INSERT INTO pages VALUES (?, ?)", (url, text))
        i += 1
