import spacy

nlp = spacy.load("fr_core_news_sm")


def segment_text(text: str, max_words: int, overlap_sentences: int) -> list[str]:
    """Découpe un texte en segments de longueur maximale.

    Args:
        text (str): texte source à segmenter.
        max_words (int): nombre maximal de mots par segment.
        overlap_sentences (int): nombre de phrases réutilisées entre deux segments.

    Returns:
        list[str]: liste des segments produits.
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
