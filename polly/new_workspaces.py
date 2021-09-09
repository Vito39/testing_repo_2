from polly.session import PollySession
from polly.errors import error_handler
import json


class Workspaces():
    def __init__(self, refresh_token=None) -> None:
        self.base_url = 'https://v2.api.polly.elucidata.io/workspaces'
        self.session = PollySession(refresh_token)

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
        print(f'Workspace Created ! \n Workspace Name = {attributes["name"]} and Workspace ID = {attributes["id"]}')

    def fetch_my_workspaces(self):
        url = self.base_url
        response = self.session.get(url)
        error_handler(response)
        for workspace in response.json()['data']:
            details = workspace['attributes']
            print(f'Workspace-Name = {details["name"]} and Workspace ID = {details["id"]}')
