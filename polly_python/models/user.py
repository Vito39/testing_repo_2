from polly_python import constants
from polly_python.models.api_handler import ApiHandler


class User():

    _id = None
    name = None
    email = None
    apiEndpoint = '/users/me'

    def __init__(self, refreshToken):
        response = ApiHandler('GET',
                              constants.V2_API_ENDPOINT + self.apiEndpoint,
                              cookies={
                                  'polly.refreshToken': refreshToken
                              }).query()

        if response.status_code == 200:
            response = response.json()['data']
            self._id = response['id']
            self.name = response['attributes']['first_name'] + ' ' + response['attributes']['last_name']
            self.email = response['attributes']['email']

    def get_name(self):
        return self.name

    def get_email(self):
        return self.email
