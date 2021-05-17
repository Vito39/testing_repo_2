import urllib.parse as urllib_parse

from polly_python import constants
from polly_python.exceptions import PollyLoginException
from polly_python.models.api_handler import ApiHandler


class Workspace():

    _id = None
    name = None
    api_endpoint = '/workspaces/'
    api_handler = ApiHandler()

    def __init__(self, workspace_id):

        from polly_python.polly_python import DEFAULT_SESSION
        if DEFAULT_SESSION is None:
            raise PollyLoginException(
                "User not logged in, run polly_python.init() ")

        self._id = workspace_id
        response = self.api_handler.send_request(
            'GET',
            url=constants.V2_API_ENDPOINT + self.api_endpoint + str(self._id),
            cookies={'polly.idToken': DEFAULT_SESSION.get_id_token()})

        if response.status_code == 200:
            response = response.json()['data']
            self.name = response['attributes']['name']

    def get_name(self):
        return self.name


class WorkspaceList():
    """
    Usage

    k = WorkspaceList()
    
    if k.next_workspaces_present():
        k.get_next_workspaces()
    """

    current_workspaces = None
    api_endpoint = '/workspaces'
    default_page_size = 2
    next_present = False
    api_handler = ApiHandler()
    page_after = None

    def __init__(self):

        from polly_python.polly_python import DEFAULT_SESSION
        if DEFAULT_SESSION is None:
            raise PollyLoginException(
                "User not logged in, run polly_python.init() ")

        self.current_workspaces = self._get_workspaces()
    
    def get_next(self):
        self.current_workspaces = self._get_workspaces()

    def get_workspaces(self):
        return self.current_workspaces

    def next_workspaces_present(self):
        return self.next_present

    def _get_workspaces(self):

        current_workspaces = []
        from polly_python.polly_python import DEFAULT_SESSION

        params = {'page[size]': self.default_page_size}
        if self.page_after:
            params['page[after]'] = self.page_after

        response = self.api_handler.send_request(
            'GET',
            url=constants.V2_API_ENDPOINT + self.api_endpoint,
            cookies={'polly.idToken': DEFAULT_SESSION.get_id_token()},
            params=params)

        if response.status_code == 200:
            response_data = response.json()['data']
            for workspace_data in response_data:
                current_workspaces.append(
                    Workspace(workspace_data['attributes']['id']))
        
        response_links = response.json()['links']

        if response_links['next']:
            self.next_present = True
            res = urllib_parse.parse_qs(response_links['next'])
            self.page_after = res['page[after]'][0]
        else:
            self.next_present = False

        return current_workspaces
