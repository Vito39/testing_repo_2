from polly_python.api_handler import ApiHandler
from polly_python.models.workspace import Workspace as WorkspaceEntity
from polly_python.constants import V2_API_ENDPOINT


class WorkspaceAPIManager():

    endpoint = V2_API_ENDPOINT + '/workspaces'
    session = None
    apiHandler = None

    def __init__(self, session):
        self.session = session
        self.apiHandler = ApiHandler()

    def get_workspace_by_id(self, workspace_id):
        if self.session is None:
            raise Exception("Session Not Defined.")
        endpoint = self.endpoint + '/{}'.format(workspace_id)
        response = self.apiHandler.dispatch_request('GET',
                                                    self.session,
                                                    url=endpoint)
        return WorkspaceEntity.from_api_response(response)
