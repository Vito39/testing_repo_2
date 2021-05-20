from polly_python import polly_python as pollypy
from polly_python.models.workspace import Workspace

def main():

    session = pollypy.init(refresh_token = 'YOUR-UNIQUE-APP-TOKEN')
    workspace_session = session.create_resource('workspaces')

    workspace = workspace_session.get_workspace_by_id(7168)
    if type(workspace) == Workspace:
        print('Workspace Object Created')
        print('Workpsace Name ----> ', workspace.get_name())

if __name__ == '__main__':
    main()
