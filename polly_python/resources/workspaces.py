from .baseresource import BaseResource
from polly_python.apimanager.workspaceApiManager import WorkspaceAPIManager


class Workspaces(BaseResource):
    """Low level client class for interacting with workspaces
    service

    """
    workspaceAPIManager = None

    def __init__(self):
        BaseResource.__init__(self)
        self.workspaceAPIManager = WorkspaceAPIManager(self.session)
        pass

    def get_all(self):
        pass

    def get_workspace_by_id(self, workspace_id: int):
        """Return workspace object represented by unique workspace_id

        Args:
            workspace_id (int): Unique workspace id

        Returns:
            Workspace: Workspace object data model
        """
        return self.workspaceAPIManager.get_workspace_by_id(workspace_id)
