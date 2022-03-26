from polly import constants as const
from polly.env import DISCOVER_API_URL
from polly.helpers import make_discover_request


class Package(object):
    """
    Package Model
    """

    REPOSITORY_PACKAGE_URL = (
        DISCOVER_API_URL + const.REPOSITORY_PACKAGE_ENDPOINT
    )

    def __init__(self, package_name, repo_id):
        """
        Create a new Package object

        Args:
            package_name (str): Package to be created
            repo_id (str): Repository ID

        Raises:
            ValueError: if package_name is not a string or is empty
            ValueError: if repo_id is not a string/int or is empty
        """
        if not isinstance(package_name, str) or not package_name:
            raise ValueError("package_name must be a str and cannot be empty")

        if (
            not isinstance(repo_id, str) and not isinstance(repo_id, int)
        ) or not repo_id:
            raise ValueError("repo_id must be a str or int and cannot be empty")

        self.package_name = package_name
        self.repo_id = str(repo_id)

        self.REPOSITORY_PACKAGE_URL = self.REPOSITORY_PACKAGE_URL.format(
            repo_id
        )

    def create(self):
        """
        Create an entry in Package table

        Raises:
            Exception: if Package already exists in the repository
            Exception: if Package creation fails

        Returns:
            package (Package): created package response
        """
        repository_package = self.get(self.repo_id)
        if repository_package is not None:
            raise Exception("package already exists for this repository")

        payload = get_package_payload(self.package_name)
        resp = make_discover_request(
            "POST", self.REPOSITORY_PACKAGE_URL, json=payload
        )
        if resp.status_code == const.CREATED:
            return resp.json()["data"].get("attributes")
        else:
            raise Exception(resp.text)

    @classmethod
    def get(cls, repo_id):
        """
        Get packages of a repository

        Args:
            repo_id (str): Repository ID

        Raises:
            Exception: if failed to get packages

        Returns:
            package : Package if exist else None
        """
        if (
            not isinstance(repo_id, str) and not isinstance(repo_id, int)
        ) or not repo_id:
            raise ValueError("repo_id must be a str or int and cannot be empty")

        resp = make_discover_request(
            "GET", cls.REPOSITORY_PACKAGE_URL.format(str(repo_id))
        )
        if resp.status_code == const.OK:
            response_data = resp.json()["data"]
            return (
                response_data[0].get("attributes")
                if len(response_data)
                else None
            )
        else:
            raise Exception(resp.text)


def get_package_payload(package_name):
    """
    Payload for Package

    Args:
        package_name (str)

    Returns:
        dict: JSON API Request Payload
    """
    return {
        "data": {
            "type": "packages",
            "attributes": {"package_name": package_name},
        }
    }
