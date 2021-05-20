from polly_python.session import SessionConfiguration
from polly_python.resources.workspaces import Workspaces
from polly_python.models.workspace import Workspace

def main():
    
    session_config = SessionConfiguration('YOUR-UNIQUE-APP-TOKEN')
    workspace_session = Workspaces(session_config)

    workspace = workspace_session.get_workspace_by_id(7168)
    if type(workspace) == Workspace:
        print('Workspace Object Created')
        print('Workpsace Name ----> ', workspace.get_name())


if __name__ == '__main__':
    main()
