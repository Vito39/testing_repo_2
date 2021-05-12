from polly_python import constants
from polly_python.models.api_handler import ApiHandler


class User():

    _id = None
    name = None
    email = None

    api_endpoint = '/users/me'
    api_handler = ApiHandler()

    def __init__(self, refresh_token):

        response = self.api_handler.send_request(
            'GET',
            url=constants.V2_API_ENDPOINT + self.api_endpoint,
            cookies={'polly.refreshToken': refresh_token})

        if response.status_code == 200:
            response = response.json()['data']
            self._id = response['id']
            self.name = response['attributes']['first_name'] + ' ' + response[
                'attributes']['last_name']
            self.email = response['attributes']['email']

    def get_name(self):
        return self.name

    def get_email(self):
        return self.email
