FROM python:3.12-slim

WORKDIR /app

# Copier et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# RUN python3 -m spacy download fr_core_news_sm

# Copier le code source
COPY ./data_searcher.py /app
COPY ./download_producer.py /app
COPY ./indexer_consumer.py /app
COPY ./indexer.py /app
COPY ./main.py /app
COPY ./producer.py /app
COPY ./sequencer.py /app
COPY ./vectoriser.py /app
COPY ./vectorizer_consumer.py /app
COPY ./warc_downloader.py /app


# Le CMD par défaut sera remplacé par la commande spécifique de chaque service dans docker-compose.yml
CMD ["python", "main.py"]
