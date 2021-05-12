from os import link
from polly_python import constants
from polly_python.models.api_handler import ApiHandler
from polly_python.__init__ import DEFAULT_SESSION

class Workspace():

    _id = None
    name = None
    apiEndpoint = '/workspace/'

    def __init__(self, workspace_id):
        self._id = workspace_id
        response = ApiHandler.send_request('GET', {
            'url': constants.V2_API_ENDPOINT + self._id,
            'cookies': {
                'polly_refreshToken': DEFAULT_SESSION.get_id_token()
            }
        })

        if response.status_code == 200:
            response = response.json()['data']
            self.name = response['attributes']['name']

    def get_name(self):
        return self.name


class WorkspaceList():

    current_workspace = None
    apiEndpoint = '/workspace'
    default_page_size = 10
    next_present = False
    # n

    def __init__(self):
        current_workspace = []
        response = ApiHandler('GET',
                              constants.V2_API_ENDPOINT + self._id,
                              cookies={
                                  'polly.refreshToken': DEFAULT_SESSION.get_id_token()
                              },
                              params={
                                  'page[size]': self.default_page_size
                              }).query()
        
        if response.status_code == 200:
            response_data = response.json()['data']
            for workspace_data in response_data:
                current_workspace.append(Workspace(workspace_data['attributes']['id']))

            response_links = response.json()['links']
            if response_links['next']:
                pass

    def get_next():
        current_workspace = []
        pass

    def get_previous():
        current_workspace = []
        pass

    def _get_workspace():