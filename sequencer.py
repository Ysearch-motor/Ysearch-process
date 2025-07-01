import spacy

nlp = spacy.load("fr_core_news_sm")


def segment_text(text: str, max_words: int, overlap_sentences: int) -> list[str]:
    """Découpe un texte en segments de longueur maximale.

    :param str text: texte source à segmenter
    :param int max_words: nombre maximal de mots par segment
    :param int overlap_sentences: nombre de phrases réutilisées entre deux segments
    :return: liste des segments produits
    :rtype: list[str]
    """
    doc = nlp(text)
    sentences = [sent.text for sent in doc.sents]
    segments = []
    actual_segment = []
    words_count = 0

    i = 0
    while i < len(sentences):
        mots = sentences[i].split()

        if words_count + len(mots) > max_words:
            segments.append(" ".join(actual_segment))

            actual_segment = actual_segment[-overlap_sentences:]
            words_count = sum(len(sent.split()) for sent in actual_segment)

        actual_segment.append(sentences[i])
        words_count += len(mots)
        i += 1

    if actual_segment:
        segments.append(" ".join(actual_segment))

    return segments
