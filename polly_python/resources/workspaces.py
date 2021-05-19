from polly_python.apimanager.workspaceApiManager import WorkspaceAPIManager


class WorkspaceResourceManager():

    configuration = None
    workspaceAPIManager = None

    def __init__(self, configuration):
        self.configuration = configuration
        self.workspaceAPIManager = WorkspaceAPIManager(self.configuration)
        pass

    def get_all(self):
        pass

    def get_workspace_by_id(self, workspace_id):
        return self.workspaceAPIManager.get_workspace_by_id(workspace_id)
