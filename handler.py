import ast
import json
import logging

from http import HTTPStatus

from train import start_training
from predict import start_prediction

from database.adapter import get_database_objects

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def train_model(event, context):
    try:
        tables = ('som_ambiente', 'efeito_sonoro', 'musica_ambiente')

        success = start_training(tables)

        if success:
            insert_status('SUCCESS', '')
            return build_response_with_success('Success')

        insert_status('ERROR', '')
        return build_response_with_error(500, 'error on send file to S3')

    except Exception as e:
        message = f'Internal error:\n{e}'
        insert_status('ERROR', message)
        logging.info(message)
        return build_response_with_error(500, message)


def predict(event, context):
    try:
        body = event['queryStringParameters']
        body = eval(str(body))

        sentence = body['sentence']
        type = body['type']

        result = start_prediction(sentence, type)

        if result:
            return build_response_with_success(result)

        return build_response(204, 'No results')

    except Exception as e:
        message = f'Internal error: {e}'
        return build_response_with_error(500, message)


def insert_status(status, message):
    conn, persistence, _ = get_database_objects()

    persistence.truncate_insert(
        table='train_status',
        status=f"'{status}'",
        message=f"'{message}'",
        created_at='now()'
    )

    conn.close()


def get_payload_params(event, *params):
    return [event[param] for param in params]


def build_transcription_response(call_data, transcription_result):
    return {
        "call_data": call_data,
        "transcription_result": transcription_result
    }


def build_response_with_success(body):
    return build_response(HTTPStatus.OK.value, body)


def build_response_with_error(code, message):
    return build_response(code, {"message": message})


def build_response(code, body):
    return {
        "isBase64Encoded": False,
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "body": json.dumps(body, ensure_ascii=False)
    }
