from polly_python.session import Polly
from polly_python.resources.workspaces import Workspaces
from polly_python.models.workspace import Workspace

def main():
    
    Polly('YOUR-API-TOKEN-HERE')

    workspace_session = Workspaces()

    workspace = workspace_session.get_workspace_by_id(7168)
    if type(workspace) == Workspace:
        print('Workspace Object Created')
        print('Workpsace Name ----> ', workspace.get_name())


if __name__ == '__main__':
    main()
