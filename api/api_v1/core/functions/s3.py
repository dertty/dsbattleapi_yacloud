import boto3
from api.api_v1.core.credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def save_file_to_s3(bfile, filename, bucket='dsbattle'):
    bfile.seek(0)
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    object = s3.Object(bucket, filename)
    object.put(Body=bfile)
    return filename


def get_file_from_s3(filename, bucket='dsbattle'):
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    object = s3.Object(bucket, filename)
    return object.get()['Body'].read()


def get_file_from_s3_bytes(filename, bucket='dsbattle'):
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    object = s3.Object(bucket, filename)
    return object.get()['Body']
