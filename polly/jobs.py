from postpy2.core import PostPython
import pathlib

curr_path = pathlib.Path(__file__).parent.absolute()

COLLECTION_PATH = f'{curr_path}/collections/jobs/jobs.postman_collection.json'
ENVIRONMENT_PATH = f'{curr_path}/collections/jobs/jobs.postman_environment.json'


class Jobs():
    def __init__(self, cookie) -> None:
        self.cookie = cookie
        self.runner = PostPython(COLLECTION_PATH)
        self.runner.environments.load(ENVIRONMENT_PATH)
        self.runner.environments.update({'cookie': self.cookie})

    def help(self):
        return self.runner.help()

    def set_project(self, project_id: int) -> None:
        self.runner.environments.update({'project_id': project_id})

    def create_job(self, project_id: int = None):
        if project_id is not None:
            self.set_project(project_id)
        response = self.runner.Requests.create_a_job()
        response.raise_for_status()
        return response.json()

    def get_all_jobs(self, project_id: int = None, limit: int = None):
        if project_id is not None:
            self.set_project(project_id)
        if limit is not None:
            self.runner.environments.update({'page_size': limit})
            response = self.runner.Requests.view_all_jobs_in_the_project_20_results_at_a_time(
            )
        else:
            response = self.runner.Requests.view_all_jobs_in_the_project()

        response.raise_for_status()
        return response.json()

    def get_job(self, job_id: str, project_id: int = None):
        if project_id is not None:
            self.set_project(project_id)
        self.runner.environments.update({'job_id': job_id})
        response = self.runner.Requests.view_a_job()
        response.raise_for_status()
        return response.json()

    def cancel_job(self, job_id: str, project_id: int = None):
        if project_id is not None:
            self.set_project(project_id)
        self.runner.environments.update({'job_id': job_id})
        response = self.runner.Requests.cancel_a_job()
        response.raise_for_status()
        return response
