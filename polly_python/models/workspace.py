from polly_python import constants
from polly_python.models.api_handler import ApiHandler


class Workspace():

    _id = None
    name = None
    apiEndpoint = '/workspace/'

    def __init__(self, workspace_id, refresh_token):
        self._id = workspace_id
        response = ApiHandler('GET',
                              constants.V2_API_ENDPOINT + self._id,
                              cookies={
                                  'polly.refreshToken': refresh_token
                              }).query()

        if response.status_code == 200:
            response = response.json()['data']
            self.name = response['attributes']['name']

    def get_name(self):
        return self.name
