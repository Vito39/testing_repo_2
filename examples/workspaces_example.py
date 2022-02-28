from polly.workspaces import Workspaces

REFRESH_TOKEN = "refresh_token"

workspace_client = Workspaces(REFRESH_TOKEN)

# List of Workspaces
list_of_workspaces = workspace_client.list_all_workspaces()
print(list_of_workspaces)

created_workspace_data = workspace_client.create_workspace(
    name="Workspace 11:44", description="Created from Postpolly"
)
workspace_id = created_workspace_data["data"]["id"]
workspace_client.set_workspace_id(workspace_id)

recently_created_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_created_data)

workspace_client.update_workspace(
    workspace_id=workspace_id,
    name="Workspace 11:50",
    description="Updated from Postpolly",
)
recently_updated_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_updated_data)

workspace_client.delete_workspace(workspace_id)
recently_deleted_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_deleted_data)
