class Workspace():

    name = None
    workspace_id = None
    active = False
    created_time = None
    creator = None
    description = None
    last_modified = None
    organisation = None

    def __init__(self,
                 workspace_id: int,
                 name: str,
                 active: bool = False,
                 creator: int = None,
                 created_time: str = None,
                 description: str = None,
                 last_modified: str = None,
                 organisation: int = None):
        self.workspace_id = workspace_id
        self.name = name
        self.active = active
        self.creator = creator
        self.created_time = created_time
        self.description = description
        self.last_modified = last_modified
        self.organisation = organisation

    def get_name(self):
        return self.name

    def get_workspace_id(self):
        return self.workspace_id

    @classmethod
    def from_api_response(cls, response):
        workspace_data = response.json()['data']['attributes']
        workspace_id = workspace_data.pop('id')
        workspace_data.pop('project_property')
        workspace_data['workspace_id'] = workspace_id
        return cls(**workspace_data)
