import functools
import os
import gzip
from multiprocessing.pool import ThreadPool

import spacy
from spacy.util import minibatch
from spacy.lang.pt.stop_words import STOP_WORDS

import pickle
import random

from database.adapter import get_database_objects
from utils.s3_transactions import send_file_to_s3


def load_tables(table):
    conn, _, query = get_database_objects()

    validations = query.query_all(
        table=table,
        column='tag, validation',
        use_where=False
    )

    conn.close()
    return validations


def get_train_data(table):
    sentence_validations = load_tables(table)

    labels = []
    sentences = []

    for label, validation in sentence_validations:
        for sentence in validation.split(';'):
            labels.append(label)
            sentences.append(sentence.strip())

    return labels, sentences


def get_train_data_not_labels(label, table):
    conn, _, query = get_database_objects()

    validations = query.query_all(
        table=f'not_{table}',
        column='validation',
        where_col='tag',
        value=label
    )

    conn.close()
    return validations

def remove_stop_words(sentence):
    filtered_sentence = [
        word
        for word in sentence.split(' ')
        if word.strip() not in STOP_WORDS
    ]
    return ' '.join(filtered_sentence)


def get_sentences_by_label(table, label):
    conn, _, query = get_database_objects()

    sentences = query.query_audio_by_tag(table, label)
    conn.close()

    return sentences


def set_label_values(label, labels, sentences):

    train_data = []

    for idx, sentence in enumerate(sentences):
        label_values = {
            'cats': {
                f'{label}': label == labels[idx],
                f'not_{label}': label != labels[idx]
            }
        }
        current_train_data = (sentence, label_values)
        train_data.append(current_train_data)

    return train_data


def train_batch_model(nlp, train_data):

    random.seed(1)
    spacy.util.fix_random_seed(1)
    optimizer = nlp.begin_training()

    losses = {}
    for epoch in range(10):
        random.shuffle(train_data)
        # Create the batch generator with batch size = 8
        batches = minibatch(train_data, size=8)
        # Iterate through minibatches
        for batch in batches:
            # Each batch is a list of (text, label) but we need to
            # send separate lists for texts and labels to update().
            # This is a quick way to split a list of tuples into lists
            texts, labels = zip(*batch)
            nlp.update(texts, labels, sgd=optimizer, losses=losses)
        # print(losses)
    return nlp


def get_new_nlp(label):
    # Create an empty model
    nlp = spacy.blank("pt")

    label_pipes = nlp.create_pipe(
        'textcat',
        config={
            "exclusive_classes": True,
            "architecture": "bow"
        }
    )
    # Adding label to model
    nlp.add_pipe(label_pipes)

    # Add labels to text classifier
    label_pipes.add_label(label)
    label_pipes.add_label(f"not_{label}")

    return nlp


def get_balanced_labeled_train_data(label, train_data):
    label_data = [
        data
        for data in train_data
        if data[1]['cats'][label]
    ]

    not_label_data = []
    size_not_label_data = len(label_data) * 4

    random.shuffle(train_data)
    for data in train_data:
        if not data[1]['cats'][label]:
            not_label_data.append(data)

            if len(not_label_data) >= size_not_label_data:
                break

    return label_data + not_label_data


def append_not_label_suggestions(train_data, label, table):
    not_label_suggestions = get_train_data_not_labels(label, table)

    if not not_label_suggestions:
        return train_data

    data_append = [
        (
            remove_stop_words(suggestion[0]),
            {
                'cats': {
                    label: False,
                    f'not_{label}': True
                }
            }
        )
        for suggestion in not_label_suggestions
    ]

    return train_data + data_append


def train_model(table):
    labels, sentences = get_train_data(table)

    for idx, sentence in enumerate(sentences):
        sentences[idx] = remove_stop_words(sentence)

    unique_labels = set(labels)

    # A nlp for each label
    for label in unique_labels:
        nlp = get_new_nlp(label)

        # Set labels/not labels
        label_sentences = set_label_values(label, labels, sentences)

        # Balance quantity of labels and not labels sentences
        label_train_data = get_balanced_labeled_train_data(label, label_sentences)

        # Append "not" label suggestions, if exists
        label_train_data = append_not_label_suggestions(label_train_data, label, table)

        # Train model in batch
        nlp_trained = train_batch_model(nlp, label_train_data)

        send_model_to_s3(nlp_trained, table, label)


def send_model_to_s3(model, table, label):
    filename = f'/tmp/{label}_nlp_trained.p'

    save_zipped_pickle(model, filename)

    bucket = os.environ.get('MODELS_S3_BUCKET')
    return send_file_to_s3(filename, bucket, folder='models', folder_s3=table)


def save_zipped_pickle(obj, filename, protocol=-1):
    with gzip.open(filename, 'wb') as f:
        pickle.dump(obj, f, protocol)


def start_training(tables):

    for table in tables:
        train_model(table)

    return True
