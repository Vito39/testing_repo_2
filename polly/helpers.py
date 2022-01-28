import boto3
import os
import requests
import logging
import pathlib
from botocore.exceptions import ClientError
from polly.constants import DATA_FORMATS
from polly.errors import (InvalidParameterException, InvalidPathException,
                          MissingKeyException, OperationFailedException, InvalidFormatException)


def check_extension(file_path: str) -> bool:
    if(not (file_path and isinstance(file_path, str))):
        raise InvalidPathException
    file_ext = pathlib.Path(file_path).suffix
    if len(file_ext) < 2:
        return False
    url = DATA_FORMATS
    f = requests.get(url)
    split_f = f.text.split("\n")
    list_f = list(split_f)
    if file_ext in list_f:
        return True
    else:
        return False


def make_path(prefix: any, postfix: any) -> str:
    if(not prefix):
        raise InvalidParameterException('prefix')
    if(not postfix):
        raise InvalidParameterException('postfix')
    return os.path.normpath(f"{prefix}/{postfix}")


def get_sts_creds(sts_dict: dict):
    if sts_dict and isinstance(sts_dict, dict):
        if 'data' in sts_dict:
            data = sts_dict.get('data')
            if('attributes' in data[0]):
                attributes = data[0].get('attributes')
                if('credentials' in attributes):
                    return attributes.get('credentials')
                else:
                    raise MissingKeyException('credentials')
            else:
                raise MissingKeyException('attributes')
        else:
            raise MissingKeyException('data')
    else:
        raise InvalidParameterException('sts_dict')


def upload_file_to_S3(local_path: str, final_path: str, credentials: dict, bucket: str) -> str:
    if(not(credentials and isinstance(credentials, dict))):
        raise InvalidParameterException('credentials')
    if(not(final_path and isinstance(final_path, str))):
        raise InvalidParameterException('final_path')
    if(not(local_path and isinstance(local_path, str))):
        raise InvalidParameterException('local_path')
    session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    s3 = session.client('s3')
    try:
        # check for file extension
        result = check_extension(local_path)
        if(result):
            final_path = make_path(final_path, local_path)
            s3.upload_file(local_path, bucket, final_path)
            return final_path
        else:
            raise InvalidFormatException
    except ClientError as e:
        logging.error(e)
        raise OperationFailedException(e)


def upload_folder_to_S3(local_path: str, final_path: str, credentials: dict, bucket: str) -> None:
    if(not(credentials and isinstance(credentials, dict))):
        raise InvalidParameterException('credentials')
    if(not(final_path and isinstance(final_path, str))):
        raise InvalidParameterException('final_path')
    if(not(local_path and isinstance(local_path, str))):
        raise InvalidParameterException('local_path')
    session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    s3 = session.client('s3')
    for roots, dirs, files in os.walk(local_path):
        for filename in files:
            result = check_extension(filename)
            if(result):
                local_p = os.path.join(roots, filename)
                s3_path = make_path(final_path, local_p)
                try:
                    s3.upload_file(local_p, bucket, s3_path)
                    logging.basicConfig(level=logging.INFO)
                    logging.info(f'Upload successful on Path={s3_path}')
                except ClientError as e:
                    logging.error(e)
                    raise OperationFailedException(e)
            else:
                logging.error(f"File format{filename} not supported")


def download_from_S3(credentials: dict, boto3_path: str, bucket: str) -> None:
    if(not(credentials and isinstance(credentials, dict))):
        raise InvalidParameterException('credentials')
    if(not(boto3_path and isinstance(boto3_path, str))):
        raise InvalidParameterException('boto3_path')
    if(not(bucket and isinstance(bucket, str))):
        raise InvalidParameterException('bucket')
    session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    s3_res = session.resource('s3')
    s3_bucket = s3_res.Bucket(bucket)
    for obj in s3_bucket.objects.filter(Prefix=boto3_path):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        try:
            s3_bucket.download_file(obj.key, obj.key)
        except ClientError as e:
            raise OperationFailedException(e)
