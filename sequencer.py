import spacy

nlp = spacy.load("fr_core_news_sm")


def segment_text(text, max_words, overlap_sentences):
    """
    This function segment the input text
    Args:
        text (string)  : the text to segment
        max_words (integer) : max length of the segment
        overlap_sentences (integer) : how many sentences will be shared by 2 segment.

    Returns:
        (list of string) : the segments
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
