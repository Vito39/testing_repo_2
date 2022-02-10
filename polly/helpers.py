import os
import json
import logging
import urllib.request
from cloudpathlib import S3Client
from botocore.exceptions import ClientError
from cmapPy.pandasGEXpress.parse_gct import parse
from polly.errors import (error_handler, InvalidParameterException, MissingKeyException,
                          InvalidPathException, OperationFailedException)
from polly.constants import CBIOPORTAL_REPO_ID, CBIOPORTAL_FIELDS


def make_path(prefix: any, postfix: any) -> str:
    """
    Function to make and return a valid path
    """
    if(not prefix):
        raise InvalidParameterException('prefix')
    if(not postfix):
        raise InvalidParameterException('postfix')
    return os.path.normpath(f"{prefix}/{postfix}")


def get_sts_creds(sts_dict: dict) -> dict:
    """
    Function to check and return temporary sts creds
    """
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


def upload_to_S3(cloud_path: str, local_path: str, credentials: dict) -> None:
    """
    Function to upload file/folder to S3 cloud path
    """
    access_key_id = credentials['AccessKeyId']
    secret_access_key = credentials['SecretAccessKey']
    session_token = credentials['SessionToken']
    client = S3Client(aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key, aws_session_token=session_token)
    source_path = client.CloudPath(cloud_path)
    if(not source_path.exists()):
        source_path.mkdir()
    try:
        source_path.upload_from(local_path, force_overwrite_to_cloud=True)
    except ClientError as e:
        raise OperationFailedException(e)


def download_from_S3(cloud_path: str, workspace_path: str, credentials: dict) -> None:
    """
    Function to download file/folder from workspaces
    """
    access_key_id = credentials['AccessKeyId']
    secret_access_key = credentials['SecretAccessKey']
    session_token = credentials['SessionToken']
    client = S3Client(aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key, aws_session_token=session_token)
    source_path = client.CloudPath(cloud_path)
    if(not source_path.exists()):
        raise InvalidPathException
    isFile = source_path.is_file()
    if(isFile):
        try:
            dest_path = os.getcwd()
            source_path.copy(dest_path, force_overwrite_to_cloud=True)
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Download successful to path={dest_path}')
        except ClientError as e:
            raise OperationFailedException(e)
    else:
        if(not cloud_path.endswith('/')):
            cloud_path += '/'
        source_path = client.CloudPath(cloud_path)
        if(not source_path.is_dir()):
            raise InvalidPathException
        try:
            dest_path = f'{make_path(os.getcwd(),workspace_path)}'
            source_path.copytree(dest_path, force_overwrite_to_cloud=True)
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Download successful to path={dest_path}')
        except ClientError as e:
            raise OperationFailedException(e)


def file_conversion(self, repo_id: str, dataset_id: str, format: str) -> None:
    download_dict = self.download_data(repo_id, dataset_id)
    if('data' in download_dict):
        data = download_dict['data']
        if('attributes' in data):
            attributes = data['attributes']
            if('download_url' in attributes):
                url = attributes['download_url']
            else:
                raise MissingKeyException('download_url')
        else:
            raise MissingKeyException('attributes')
    else:
        raise MissingKeyException('data')
    file_name = f"{dataset_id}.gct"
    try:
        urllib.request.urlretrieve(url, file_name)
        data = parse(file_name)
        os.remove(file_name)
        row_metadata = data.row_metadata_df
        if(repo_id == CBIOPORTAL_REPO_ID):
            row_metadata = row_metadata.rename(CBIOPORTAL_FIELDS, axis=1)
        row_metadata.to_csv(f"{dataset_id}.{format}", sep="\t")
    except Exception as e:
        raise OperationFailedException(e)


def get_index_name(self, url: str) -> str:
    if(not (url and isinstance(url, str))):
        raise InvalidParameterException('url')
    response = self.session.get(url)
    error_handler(response)
    file_index = response.json().get('data', {}).get('attributes', {}).get('indexes', {}).get('files')
    if not file_index:
        raise MissingKeyException("indexes")
    return file_index


def get_data_type(self, url: str, payload: dict) -> str:
    if(not (url and isinstance(url, str))):
        raise InvalidParameterException('url')
    if(not (payload and isinstance(payload, dict))):
        raise InvalidParameterException('payload')
    response = self.session.post(url, data=json.dumps(payload))
    error_handler(response)
    response_data = response.json()
    hits = response_data.get("hits", {}).get("hits")
    if not hits:
        raise MissingKeyException("hits")
    dataset = hits[0]
    data_type = dataset.get("_source", {}).get("kw_data_type")
    if not data_type:
        raise MissingKeyException("data_type")
    return data_type
