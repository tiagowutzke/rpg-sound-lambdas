import os
import boto3


def send_file_to_s3(filename, bucket, folder='audios', folder_s3=None):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('ACCESS'),
        aws_secret_access_key=os.environ.get('SECRET')
    )

    file = f"./{folder}/{filename}" if folder is 'audios' else filename

    s3_file = filename.rsplit('/', 1)[-1]

    if folder_s3:
        s3_file = f'{folder_s3}/{s3_file}'

    s3_params = {
        'ACL': 'public-read',
        'ContentType': 'audio/mpeg'
    } if folder is 'audios' else {}

    try:
        s3.upload_file(file, bucket, s3_file, ExtraArgs=s3_params)
        return True
    except Exception as e:
        print(f"Error on send file to s3: {e}")
        return False


def get_file_from_s3(bucket, key, download_path=None):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('ACCESS'),
        aws_secret_access_key=os.environ.get('SECRET')
    )
    download_path = download_path or key

    response = s3.download_file(bucket, key, f'/tmp/{download_path}')

    return response


def get_files_from_s3(bucket, folder):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('ACCESS'),
        aws_secret_access_key=os.environ.get('SECRET')
    )
    return s3.list_objects(Bucket=bucket, Prefix=folder)
