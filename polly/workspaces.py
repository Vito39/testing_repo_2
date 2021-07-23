import pathlib
from postpy2.core import PostPython

curr_path = pathlib.Path(__file__).parent.absolute()

COLLECTION_PATH = f'{curr_path}/collections/workspaces/workspaces.postman_collection.json'
ENVIRONMENT_PATH = f'{curr_path}/collections/workspaces/workspaces.postman_environment.json'


class Workspaces():
    def __init__(self, refresh_token) -> None:
        self.id_token = None
        self.runner = PostPython(COLLECTION_PATH)
        self.runner.environments.load(ENVIRONMENT_PATH)
        self.runner.environments.update({'refresh_token': refresh_token})

    def _set_id_token(self, response):
        self.id_token = response.cookies.get_dict()['polly.idToken']
        self.runner.environments.update({'id_token': self.id_token})

    def set_workspace_id(self, workspace_id):
        self.runner.environments.update({'workspace_id': workspace_id})

    def list_all_workspaces(self):
        response = self.runner.Workspaces.list_all_workspaces()
        response.raise_for_status()
        self._set_id_token(response)
        return response.json()

    def get_workspace_by_id(self, workspace_id=None):
        if self.id_token is None:
            _ = self.list_all_workspaces()
        if workspace_id is not None:
            self.set_workspace_id(workspace_id)
        response = self.runner.Workspaces.view_recently_created_workspace()
        response.raise_for_status()
        return response.json()

    def create_workspace(self, name=None, description=None):
        if self.id_token is None:
            _ = self.list_all_workspaces()
        if name is not None:
            self.runner.environments.update({'test_workspace_name': name})
        if description is not None:
            self.runner.environments.update(
                {'new_test_workspace_description': description})
        response = self.runner.Workspaces.create_a_workspace()
        response.raise_for_status()
        return response.json()

    def update_workspace(self, name=None, description=None, workspace_id=None):
        if self.id_token is None:
            _ = self.list_all_workspaces()
        if workspace_id is not None:
            self.set_workspace_id(workspace_id)
        if name is not None:
            self.runner.environments.update(
                {'update_test_workspace_name': name})
        if description is not None:
            self.runner.environments.update(
                {'update_test_workspace_description': description})
        response = self.runner.Workspaces.update_a_workspace_with_name_and_description(
        )
        response.raise_for_status()
        return response

    def delete_workspace(self, workspace_id=None):
        if self.id_token is None:
            _ = self.list_all_workspaces()
        if workspace_id is not None:
            self.set_workspace_id(workspace_id)
        response = self.runner.Workspaces.delete_a_workspace()
        response.raise_for_status()
        return response
