from polly.auth import Polly
from polly.errors import InvalidParameterException, error_handler, InvalidPathException
from polly import helpers
import logging
import pandas as pd
import json
import os


class Workspaces:
    def __init__(self, token=None, env="polly") -> None:
        self.session = Polly.get_session(token, env=env)
        self.base_url = f"https://v2.api.{self.session.env}.elucidata.io"
        self.resource_url = f"{self.base_url}/workspaces"
        if self.session.env == "polly":
            self.env_string = "prod"
        elif self.session.env == "testpolly":
            self.env_string = "test"
        else:
            self.env_string = "devenv"

    def create_workspace(self, name: str, description=None):
        url = self.resource_url
        payload = {
            "data": {
                "type": "workspaces",
                "attributes": {
                    "name": name,
                    "description": description,
                    "project_property": {"type": "workspaces", "labels": ""},
                },
            }
        }
        response = self.session.post(url, data=json.dumps(payload))
        error_handler(response)
        attributes = response.json()["data"]["attributes"]
        logging.basicConfig(level=logging.INFO)
        logging.info("Workspace Created !")
        return attributes

    def fetch_my_workspaces(self):
        url = self.resource_url
        response = self.session.get(url)
        error_handler(response)
        pd.set_option("display.max_columns", 20)
        dataframe = pd.DataFrame.from_dict(
            pd.json_normalize(response.json()["data"]), orient="columns"
        )
        return dataframe

    def upload_to_workspaces(
        self, workspace_id: int, workspace_path: str, local_path: str
    ) -> None:
        """
        Function to upload files/folders to workspaces
        """
        if not (workspace_id and isinstance(workspace_id, int)):
            raise InvalidParameterException("workspace_id")
        if not (local_path and isinstance(local_path, str)):
            raise InvalidParameterException("local_path")
        if not (workspace_path and isinstance(workspace_path, str)):
            raise InvalidParameterException("workspace_path")
        isExists = os.path.exists(local_path)
        if not isExists:
            raise InvalidPathException
        sts_url = f"{self.base_url}/projects/{workspace_id}/credentials/files"
        creds = self.session.get(sts_url)
        error_handler(creds)
        credentials = helpers.get_sts_creds(creds.json())
        bucket = f"mithoo-{self.env_string}-project-data-v1"
        s3_path = f"{bucket}/{workspace_id}/"
        s3_path = f"s3://{helpers.make_path(s3_path, workspace_path)}"
        helpers.upload_to_S3(s3_path, local_path, credentials)
        logging.basicConfig(level=logging.INFO)
        logging.info(f"Upload successful on workspace-id={workspace_id}.")

    def download_from_workspaces(self, workspace_id: int, workspace_path: str) -> None:
        """
        Function to download files/folders from workspaces
        """
        if not (workspace_id and isinstance(workspace_id, int)):
            raise InvalidParameterException("workspace_id")
        if not (workspace_path and isinstance(workspace_path, str)):
            raise InvalidParameterException("workspace_path")
        sts_url = f"{self.base_url}/projects/{workspace_id}/credentials/files"
        creds = self.session.get(sts_url)
        error_handler(creds)
        credentials = helpers.get_sts_creds(creds.json())
        bucket = f"mithoo-{self.env_string}-project-data-v1"
        s3_path = f"{bucket}/{workspace_id}/"
        s3_path = f"s3://{helpers.make_path(s3_path, workspace_path)}"
        helpers.download_from_S3(s3_path, workspace_path, credentials)
