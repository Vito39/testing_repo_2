from polly.auth import Polly
from polly.errors import (InvalidParameterException, error_handler,
                          InvalidPathException)
from polly import helpers
from cloudpathlib import S3Client
import logging
import pandas as pd
import json
import os


class Workspaces():
    def __init__(self, token=None, env='polly') -> None:
        self.session = Polly.get_session(token, env=env)
        self.base_url = f"https://v2.api.{self.session.env}.elucidata.io"
        self.resource_url = f"{self.base_url}/workspaces"
        if(self.session.env == 'polly'):
            self.bucket = 'mithoo-prod-project-data-v1'
        elif(self.session.env == 'testpolly'):
            self.bucket = 'mithoo-test-project-data-v1'
        else:
            self.bucket = 'mithoo-devenv-project-data-v1'

    def create_workspace(self, name: str, description=None):
        url = self.resource_url
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
        url = self.resource_url
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
        if(not (workspace_id and isinstance(workspace_id, int))):
            raise InvalidParameterException('workspace_id')
        if(not (local_path and isinstance(local_path, str))):
            raise InvalidParameterException('local_path')
        if(not (workspace_path and isinstance(workspace_path, str))):
            raise InvalidParameterException('workspace_path')
        isExists = os.path.exists(local_path)
        if(not isExists):
            raise InvalidPathException
        isFile = os.path.isfile(local_path)
        sts_url = f'{self.base_url}/projects/{workspace_id}/credentials/files'
        creds = self.session.get(sts_url)
        error_handler(creds)
        credentials = helpers.get_sts_creds(creds.json())
        final_path = f"{helpers.make_path(workspace_id, workspace_path)}"
        if(isFile):
            uploaded_path = helpers.upload_file_to_S3(local_path, final_path, credentials, self.bucket)
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Upload successful on workspace-id={workspace_id}. Path={uploaded_path}')
        else:
            helpers.upload_folder_to_S3(local_path, final_path, credentials, self.bucket)

    def download_from_workspaces(self, workspace_id: int, workspace_path: str) -> None:
        '''
        Function to download file/folder from workspaces.
        Input: workspace_id: int, workspace_path: str
        Output: Confirmation status for download.
        '''
        if(not (workspace_path and isinstance(workspace_path, str))):
            raise InvalidParameterException('workspace_path')
        if(not (workspace_id and isinstance(workspace_id, int))):
            raise InvalidParameterException('workspace_id')
        sts_url = f'{self.base_url}/projects/{workspace_id}/credentials/files'
        creds = self.session.get(sts_url)
        error_handler(creds)
        credentials = helpers.get_sts_creds(creds.json())
        access_key_id = credentials['AccessKeyId']
        secret_access_key = credentials['SecretAccessKey']
        session_token = credentials['SessionToken']
        client = S3Client(aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key, aws_session_token=session_token)
        boto3_path = f"{helpers.make_path(workspace_id, workspace_path)}"
        s3_path = f"s3://{self.bucket}/{boto3_path}"
        source_path = client.CloudPath(s3_path)
        isfile = source_path.is_file()
        isdir = source_path.is_dir()
        if(isdir):
            # For appending the path with / if its a folder
            if(not boto3_path.endswith('/')):
                boto3_path += '/'
                s3_path += '/'
                isdir = client.CloudPath(s3_path).is_dir()
        if(isfile or isdir):
            helpers.download_from_S3(credentials, boto3_path, self.bucket)
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Download successful from workspace-id={workspace_id}.')
        else:
            raise InvalidPathException
