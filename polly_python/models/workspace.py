class Workspace():
    """Data model class for workspace object
    """

    name = None
    id = None
    active = False
    created_time = None
    creator = None
    description = None
    last_modified = None
    organisation = None

    def __init__(self,
                 name: str,
                 id: str = None,
                 active: bool = False,
                 creator: int = None,
                 created_time: str = None,
                 description: str = None,
                 last_modified: str = None,
                 organisation: int = None):
        self.id = id
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
        return self.id

    @classmethod
    def from_api_response(cls, response):
        """Loads and Returns workpace object from the
        api response

        Args:
            response (Response): Response object from request library

        Returns:
            Workspace: Class object loaded with data from api response
        """
        workspace_data = response.json()['data']['attributes']
        workspace_data.pop('project_property')
        return cls(**workspace_data)
