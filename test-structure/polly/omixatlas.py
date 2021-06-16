from .core import PostPython
from json import dumps
import pathlib

curr_path = pathlib.Path(__file__).parent.absolute()

COLLECTION_PATH = f'{curr_path}/collections/omixatlas/repository_packages.postman_collection.json'
ENVIRONMENT_PATH = f'{curr_path}/collections/omixatlas/repositories.postman_environment.json'


class OmixAtlas():
    def __init__(self, refresh_token) -> None:
        self.refresh_token = refresh_token
        self.runner = PostPython(COLLECTION_PATH)
        self.runner.environments.load(ENVIRONMENT_PATH)
        self.runner.environments.update({'refreshToken': refresh_token})

    def get_all_repos(self):
        response = self.runner.Requests.repositories_get_all()
        response.raise_for_status()
        return response.json()

    def get_all_packages(self, repo_id):
        self.runner.environments.update({'repositoryId': repo_id})
        response = self.runner.Requests.repository_package_get_all()
        response.raise_for_status()
        return response.json()

    def get_package(self, repo_id, package):
        self.runner.environments.update({
            'repositoryId': repo_id,
            'packageNametoCreate': package
        })
        response = self.runner.repository_package_get_one()
        response.raise_for_status()
        return response.json()

    def create_package(self, repo_id):
        self.runner.environments.update({'repositoryId': repo_id})
        response = self.runner.Requests.repository_package_create()
        response.raise_for_status()
        return response.json()

    def delete_package(self, repo_id, package):
        self.runner.environments.update({
            'repositoryId': repo_id,
            'packageNametoCreate': package
        })
        response = self.runner.Requests.repository_package_delete()
        response.raise_for_status()
        return response

    def elastic_query(self, index, query):
        self.runner.environments.update({
            'index': index,
            'query': dumps(query)
        })

        response = self.runner.Requests.elastic_search()
        response.raise_for_status()
        return response.json()
