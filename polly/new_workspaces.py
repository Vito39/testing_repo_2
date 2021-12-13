from polly.polly import Polly
from polly.errors import error_handler
from polly.constants import V2_API_ENDPOINT
import logging
import pandas as pd
import json


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
