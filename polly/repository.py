import json
from polly.auth import Polly
from polly.helpers import make_discover_request
# from polly.env import DISCOVER_API_URL
from polly import constants as const
# from polly.package import Package
from polly.errors import error_handler
from polly import repository_validators as validator

class Repository(object):
    """
    Repository Model
    """

    # _REPOSITORY_URL = DISCOVER_API_URL + const.REPOSITORIES_ENDPOINT

    def __init__(self, token=None, env="polly") -> None:
        self.session = Polly.get_session(token, env=env)
        self.discover_url = f"https://api.discover.{self.session.env}.elucidata.io"
        self._frontend_info = {}
        self._components = []
        self._studio_presets = []
        self._repo_id = None
        self._repo_name = None

    @property
    def repo_id(self):
        return self._repo_id

    @property
    def repo_name(self):
        return self._repo_name

    @property
    def frontend_info(self):
        return self._frontend_info

    @frontend_info.setter
    def frontend_info(self, info: dict):
        # we need all keys of frontend_info, when we're creating a repo
        validator.validate_frontend_info(info)

        for key, value in info.items():
            if value is not None:
                self._frontend_info[key] = value

        # TODO: removed once we made changes in API for using repo_id as
        # repo_name for indexes
        if not self._repo_name:
            self._repo_name = (
                self._frontend_info["display_name"].lower().replace(" ", "_")
            )

    @property
    def components(self):
        return self._components

    @components.setter
    def components(self, input: dict):
        validator.validate_components_input(input)

        self._components = [
            {"data_type": datatypes, "component_id": component_id}
            for component_id, datatypes in input.items()
        ]

    @property
    def studio_presets(self):
        return self._studio_presets

    @studio_presets.setter
    def studio_presets(self, input):
        validator.validate_studio_presets_input(input)

        self._studio_presets = [
            {"data_type": datatypes, "preset_id": preset_id}
            for preset_id, datatypes in input.items()
        ]

    def _create(self):
        """
        Create an entry in Repository table

        Raises:
            Exception: if Repository creation failed

        Returns:
            repository (Repository): created repository
        """
        if self.repo_id:
            raise Exception("Error: Repository already exists")
        print("----1------")
        payload = get_repository_payload()
        payload["data"]["attributes"]["repo_name"] = self.repo_name
        payload["data"]["attributes"]["frontend_info"] = self.frontend_info
        payload["data"]["attributes"]["components"] = self.components
        payload["data"]["attributes"]["studio_presets"] = self.studio_presets
        print("-----2-------")
        indexes = payload["data"]["attributes"]["indexes"]
        for key in indexes.keys():
            indexes[key] = f"{self.repo_name}_{key}"
        print("-----3-------")
        validator.validate_repository_schema(payload["data"]["attributes"])
        repository_url = f"{self.discover_url}{const.REPOSITORIES_ENDPOINT}"
        print(f"---repository url----{repository_url}---")
        resp = self.session.post(repository_url, json=payload)
        print("-----4-------")
        error_handler(resp)
        # resp = make_discover_request("POST", repository_url, json=payload)
        print("-----5-------")
        if resp.status_code != const.CREATED:
            raise Exception(resp.text)
        else:
            self._repo_id = resp.json()["data"]["id"]

    def _update(self):
        """
        Updates an existing repository

        Raises:
            Exception: if repository not exists
            Exception: failed to update repository
        """
        if not self.repo_id:
            raise Exception("Error: Trying to update non-existing repository")

        payload = get_repository_payload()
        del payload["data"]["attributes"]["repo_name"]
        del payload["data"]["attributes"]["indexes"]
        payload["data"]["id"] = self.repo_id
        payload["data"]["attributes"]["frontend_info"] = self.frontend_info
        payload["data"]["attributes"]["components"] = self.components
        payload["data"]["attributes"]["studio_presets"] = self.studio_presets

        validator.validate_repository_schema(
            payload["data"]["attributes"], update=True
        )

        repository_url = f"{self.discover_url}{const.REPOSITORIES_ENDPOINT}"
        resp = self.session.patch(repository_url)
        error_handler(resp)
        # resp = make_discover_request(
        #     "PATCH", f"{repository_url}/{self.repo_id}", json=payload
        # )
        if resp.status_code != const.OK:
            raise Exception(resp.text)

    def save(self):
        """
        Create or Update a Repository
        """
        return self._update() if self.repo_id else self._create()

    
    def get(self, repo_id):
        """
        Return a Repository by a given ID

        Args:
            repo_id (int): Repository ID

        Raises:
            Exception: if failed to get repository

        Returns:
            repo (Repository): Repository object
        """
        repository_url = f"{self.discover_url}{const.REPOSITORIES_ENDPOINT}"
        response = self.session.post(repository_url)
        error_handler(response)
        # response = make_discover_request(
        #     "GET", f"{repository_url}/{repo_id}"
        # )
        if response.status_code != const.OK:
            raise Exception(response.text)

        return response.json()["data"]["attributes"]
        # return cls.as_object(response.json()["data"]["attributes"])

    # def add_package(self, package_name):
    #     """
    #     Create a new package in the repository

    #     Args:
    #         package_name (str): Package to be created

    #     Returns:
    #         package (Package): created package
    #     """
    #     package = Package(package_name, self.repo_id)
    #     return package.create()

    # def get_package(self):
    #     """
    #     Return Package for current Repository

    #     Returns:
    #         dict: Package
    #     """
    #     package = Package.get(self.repo_id)
    #     if package is None:
    #         raise Exception("No package exists for this repository")
    #     return package

    @staticmethod
    def as_object(repo: dict):
        """
        Convert dict into a Repository object

        Args:
            repo (dict): dict representation of a repository

        Returns:
            repository (Repository): Repository instance
        """
        if repo.get("indexes"):
            del repo["indexes"]
        validator.validate_repository_schema(repo)

        repo_obj = Repository()
        repo_obj._repo_id = repo["repo_id"]
        repo_obj._repo_name = repo["repo_name"]
        repo_obj._frontend_info = repo["frontend_info"]
        repo_obj._components = repo.get("components")
        repo_obj._studio_presets = repo.get("studio_presets")
        return repo_obj

    def as_dict(self):
        """
        Returns this repository as a dict

        Returns:
            dict: Repository as a dict
        """
        return {
            "repo_id": self.repo_id,
            "repo_name": self.repo_name,
            "frontend_info": self.frontend_info,
            "components": self.components,
            "studio_presets": self.studio_presets,
        }

    def __repr__(self):
        return json.dumps(self.as_dict())


def get_repository_payload():
    """
    Payload for Repository

    Returns:
        dict: JSON API Request Payload
    """
    return {
        "data": {
            "type": "repositories",
            "attributes": {
                "frontend_info": {
                    "description": "<DESCRIPTION>",
                    "display_name": "<REPO_DISPLAY_NAME>",
                    "explorer_enabled": True,
                    "initials": "<INITIALS>",
                },
                "indexes": {
                    "csv": "<REPO_NAME>_csv",
                    "files": "<REPO_NAME>_files",
                    "gct_data": "<REPO_NAME>_gct_data",
                    "gct_metadata": "<REPO_NAME>_gct_metadata",
                    "h5ad_data": "<REPO_NAME>_h5ad_data",
                    "h5ad_metadata": "<REPO_NAME>_h5ad_metadata",
                    "ipynb": "<REPO_NAME>_ipynb",
                    "json": "<REPO_NAME>_json",
                },
                "repo_name": "<REPO_NAME>",
            },
        }
    }
