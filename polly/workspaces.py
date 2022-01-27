from polly.auth import Polly
from polly.errors import (error_handler, OperationFailedError,
                          InvalidPathError)
from polly.constants import V2_API_ENDPOINT, WORKSPACE_BUCKET
from polly import helpers
from cloudpathlib import S3Client
from botocore.exceptions import ClientError
import logging
import pandas as pd
import json
import boto3
import os


class Workspaces():
    def __init__(self, token=None) -> None:
        self.base_url = f'{V2_API_ENDPOINT}/workspaces'
        self.session = Polly.get_session(token)

    def create_workspace(self, name: str, description=None):
        url = self.base_url
        payload = {
            "data": {"type": "workspaces",
                     "attributes": {"name": name, "description": description,
                                    "project_property": {"type": "workspaces", "labels": ""}}}
            }
        response = self.session.post(url, data=json.dumps(payload))
        error_handler(response)
        attributes = response.json()['data']['attributes']
        logging.basicConfig(level=logging.INFO)
        logging.info('Workspace Created !')
        return attributes

    def fetch_my_workspaces(self):
        url = self.base_url
        response = self.session.get(url)
        error_handler(response)
        pd.set_option("display.max_columns", 20)
        dataframe = pd.DataFrame.from_dict(pd.json_normalize(response.json()['data']), orient='columns')
        return dataframe

    def upload_to_workspaces(self, workspace_id: int, local_path: str, workspace_path: str) -> None:
        """
        Function to upload file/folder to workspaces.
        Input: workspace_id: int,
        workspace_path:str(Non-default argument), local_path: str
        Output: Confirmation status for upload
        """
        isExists = os.path.exists(local_path)
        if(not isExists):
            raise InvalidPathError
        isFile = os.path.isfile(local_path)
        sts_url = f'{V2_API_ENDPOINT}/projects/{workspace_id}/credentials/files'
        creds = self.session.get(sts_url)
        access_key_id = creds.json()['data'][0]['attributes']['credentials']['AccessKeyId']
        secret_access_key = creds.json()['data'][0]['attributes']['credentials']['SecretAccessKey']
        session_token = creds.json()['data'][0]['attributes']['credentials']['SessionToken']
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token
        )
        s3 = session.client('s3')
        if(workspace_path):
            final_path = f"{helpers.make_path(workspace_id, workspace_path)}"
        else:
            final_path = f"{workspace_id}/"
        if(isFile):
            try:
                # check for file extension
                result = helpers.check_extension(local_path)
                if(result):
                    final_path = helpers.make_path(final_path, local_path)
                    s3.upload_file(local_path, WORKSPACE_BUCKET, final_path)
                    logging.basicConfig(level=logging.INFO)
                    logging.info(f'Upload successful on workspace-id={workspace_id}. Path={final_path}')
                else:
                    logging.error("File format not supported")
            except ClientError as e:
                logging.error(e)
                raise OperationFailedError(e)
        else:
            for roots, dirs, files in os.walk(local_path):
                for filename in files:
                    result = helpers.check_extension(filename)
                    if(result):
                        local_p = os.path.join(roots, filename)
                        s3_path = helpers.make_path(final_path, local_p)
                        try:
                            s3.upload_file(local_p, WORKSPACE_BUCKET, s3_path)
                        except ClientError as e:
                            logging.error(e)
                            raise OperationFailedError(e)
                    else:
                        logging.error(f"File format{filename} not supported")
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Upload successful on workspace-id={workspace_id}. Path={final_path}')

    def download_from_workspaces(self, workspace_id: int, workspace_path: str) -> None:
        '''
        Function to download file/folder from workspaces.
        Input: workspace_id: int, workspace_path: str
        Output: Confirmation status for download.
        '''
        if(not workspace_path):
            raise InvalidPathError
        sts_url = f'{V2_API_ENDPOINT}/projects/{workspace_id}/credentials/files'
        creds = self.session.get(sts_url)
        access_key_id = creds.json()['data'][0]['attributes']['credentials']['AccessKeyId']
        secret_access_key = creds.json()['data'][0]['attributes']['credentials']['SecretAccessKey']
        session_token = creds.json()['data'][0]['attributes']['credentials']['SessionToken']
        client = S3Client(aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key, aws_session_token=session_token)
        boto3_path = f"{helpers.make_path(workspace_id, workspace_path)}"
        s3_path = f"s3://{WORKSPACE_BUCKET}/{boto3_path}"
        source_path = client.CloudPath(s3_path)
        isfile = source_path.is_file()
        isdir = source_path.is_dir()
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token
        )
        s3_res = session.resource('s3')
        bucket = s3_res.Bucket(WORKSPACE_BUCKET)
        if(isdir):
            if(not boto3_path.endswith('/')):
                boto3_path += '/'
                s3_path += '/'
                isdir = client.CloudPath(s3_path).is_dir()
        if(isfile or isdir):
            for obj in bucket.objects.filter(Prefix=boto3_path):
                if not os.path.exists(os.path.dirname(obj.key)):
                    os.makedirs(os.path.dirname(obj.key))
                try:
                    bucket.download_file(obj.key, obj.key)
                except ClientError as e:
                    raise OperationFailedError(e)
                logging.basicConfig(level=logging.INFO)
                logging.info(f'Download successful from workspace-id={workspace_id}.')
        else:
            raise InvalidPathError
