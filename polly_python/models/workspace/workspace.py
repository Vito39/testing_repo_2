from os import link
from polly_python import constants
from polly_python.exceptions import PollyLoginException
from polly_python.models.api_handler import ApiHandler
from polly_python.polly_python import DEFAULT_SESSION


class Workspace():

    _id = None
    name = None
    apiEndpoint = '/workspace/'

    def __init__(self, workspace_id):

        if DEFAULT_SESSION is None:
            raise PollyLoginException("User not logged in, run polly_python.init() ")

        self._id = workspace_id
        response = ApiHandler.send_request(
            'GET',
            url=constants.V2_API_ENDPOINT + self._id,
            cookies={'polly_refreshToken': DEFAULT_SESSION.get_id_token()})

        if response.status_code == 200:
            response = response.json()['data']
            self.name = response['attributes']['name']

    def get_name(self):
        return self.name


class WorkspaceList():
    """
    Usage

    k = WorkspaceList()
    k.get_workspaces()
    if k.next_workspaces_present():
        k.get_next_workspaces()
    """

    current_workspace = None
    apiEndpoint = '/workspace'
    default_page_size = 10
    next_present = False

    def __init__(self):

        if DEFAULT_SESSION is None:
            raise PollyLoginException("User not logged in, run polly_python.init() ")

        current_workspace = []
        response = ApiHandler('GET',
                              constants.V2_API_ENDPOINT + self._id,
                              cookies={
                                  'polly.refreshToken':
                                  DEFAULT_SESSION.get_id_token()
                              },
                              params={
                                  'page[size]': self.default_page_size
                              }).query()

        if response.status_code == 200:
            response_data = response.json()['data']
            for workspace_data in response_data:
                current_workspace.append(
                    Workspace(workspace_data['attributes']['id']))

            response_links = response.json()['links']
            if response_links['next']:
                pass

    def get_next(self):
        current_workspace = []
        pass

    def get_workspaces(self):
        return self.current_workspaces
    
    def next_workspaces_present(self):
        return self.next_present
