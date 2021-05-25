from polly_python.session import Polly
from polly_python.resources.workspaces import Workspaces
from polly_python.models.workspace import Workspace

REFRESH_TOKEN = "YOUR-REFRESH-TOKEN"

WORKSPACE_ID = 7237


def main():

    Polly(REFRESH_TOKEN)

    workspace_session = Workspaces()

    workspace = workspace_session.get_workspace_by_id(WORKSPACE_ID)
    if type(workspace) == Workspace:
        print("Workspace Object Created")
        print("Workspace Name ----> ", workspace.get_name())

    print("\nAs JSON:")
    workspace_json = workspace.to_json()
    print(workspace_json)

    print("\nAs a DataFrame:")
    workspace_df = workspace.to_df()
    print(workspace_df)


if __name__ == "__main__":
    main()
