from polly_python.models.base_class import BaseClass


class Workspace(BaseClass):
    """Data model class for workspace object"""

    name = None
    workspace_id = None
    active = False
    created_time = None
    creator = None
    description = None
    last_modified = None
    organisation = None

    def __init__(
        self,
        name: str,
        workspace_id: str = None,
        active: bool = False,
        creator: int = None,
        created_time: str = None,
        description: str = None,
        last_modified: str = None,
        organisation: int = None,
    ):
        super().__init__()
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
        """Loads and Returns workpace object from the
        api response

        Args:
            response (Response): Response object from request library

        Returns:
            Workspace: Class object loaded with data from api response
        """
        workspace_data = response.json()["data"]["attributes"]
        workspace_id = workspace_data.pop("id")
        workspace_data.pop("project_property")
        workspace_data["workspace_id"] = workspace_id
        return cls(**workspace_data)
