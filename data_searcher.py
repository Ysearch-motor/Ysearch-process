from warcio.archiveiterator import ArchiveIterator
import trafilatura
from langdetect import detect
from bs4 import BeautifulSoup


def get_data(warc_file):
    """
    Extract data from a WARC file and return a list of lists containing the URL, the h1 and the text of the page.
    Args:
        warc_file (str): the path to the compressed WARC file.

    Returns:
        list of lists: the data extracted from the WARC file.
    """

    data = []
    i = 0
    with open(warc_file, "rb") as f:
        for record in ArchiveIterator(f):
            if record.rec_type == "response":
                url = record.rec_headers.get_header("WARC-Target-URI")
                html = record.content_stream().read().decode(errors="ignore")
                text_brut = trafilatura.extract(html)
                if text_brut:
                    try:
                        if detect(text_brut) == "fr":
                            soup = BeautifulSoup(html, "html.parser")
                            h1 = soup.h1.get_text() if soup.h1 else ""
                            data.append([[url], [h1], [text_brut]])
                            i += 1
                            print(i)
                    except Exception as e:
                        print(f"Skipping record due to error: {e}")

        # Write the data for keep trace
        with open("data.txt", "w", encoding="utf-8") as f:
            for row in data:
                f.write(f"{row},\n")

        return data
