import os
import gzip
import pickle

from utils.s3_transactions import get_files_from_s3, get_file_from_s3

from spacy.lang.pt.stop_words import STOP_WORDS

from database.adapter import get_database_objects


def remove_stop_words(sentence):
    filtered_sentence = [
        word
        for word in sentence.split(' ')
        if word.strip() not in STOP_WORDS
    ]
    return ' '.join(filtered_sentence)


def nlp_predict(nlp, sentence):
    sentence = [remove_stop_words(sentence)]
    docs = [nlp.tokenizer(text) for text in sentence]

    # Use textcat to get the scores for each doc
    textcat = nlp.get_pipe('textcat')
    scores, _ = textcat.predict(docs)

    labels_classified = scores.argmax(axis=1)
    label_predicted = ([textcat.labels[label] for label in labels_classified])[0]

    if 'not' not in label_predicted:
        return label_predicted, scores.max(axis=1)[0]

    return None


def check_multiple_prediction(predictions, valid_score):
    try:
        valid_score = valid_score or 0.55

        valid_predictions = np.array([
            (label, score)
            for label, score in predictions
            if score >= valid_score
        ])

        labels, scores = zip(*valid_predictions)

        max_score_idx = scores.index(max(scores))

        return labels[max_score_idx], max(scores)
    except:
        return '', ''


def check_single_prediction(predictions, valid_score):
    try:
        valid_score = valid_score or 0.53

        label, score = predictions[0]

        if score >= valid_score:
            return label, score

        return '', ''
    except:
        return '', ''


def make_suggestions(sentence, prediction_to_check, label_predicted, valid_score):
    valid_score = valid_score or 0.52

    sugestion_labels, sugestion_scores = zip(*prediction_to_check)

    return [
        (label, score, sentence)
        for label, score in zip(sugestion_labels, sugestion_scores)
        if label is not label_predicted and score >= valid_score
    ]


def load_zipped_pickle(filename):
    with gzip.open(filename, 'rb') as f:
        loaded_object = pickle.load(f)
        return loaded_object


def start_prediction(sentence, table):
    bucket_name = os.environ.get('MODELS_S3_BUCKET')
    bucket = get_files_from_s3(bucket_name, table)

    predictions_to_check = []

    for file in bucket.get('Contents', []):
        key = file.get('Key')
        file = key.replace(table+'/', '')

        get_file_from_s3(bucket_name, key, file)
        nlp = load_zipped_pickle(f'/tmp/{file}')

        prediction = nlp_predict(nlp, sentence)

        if not prediction:
            continue
        else:
            predictions_to_check.append(prediction)

    conn, _, query = get_database_objects()
    _, _, score_single, score_multiple, score_suggestions = query.query_config()
    conn.close()

    if not predictions_to_check:
        return None

    # Check if score prediction is ok
    if len(predictions_to_check) > 1:
        label_predicted, score_predicted = check_multiple_prediction(predictions_to_check, score_multiple)
    else:
        label_predicted, score_predicted = check_single_prediction(predictions_to_check, score_single)

    # Keeping suggestions to improve model
    suggestions = make_suggestions(sentence, predictions_to_check, label_predicted, score_suggestions)

    return label_predicted, str(score_predicted), str(suggestions)
