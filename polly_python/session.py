from polly_python.exceptions import MalFormedConfigurationException
from polly_python.resourcemanager import RESORUCE_CLASS_MAPPING


class SessionConfiguration():

    refresh_token = None

    def __init__(self, refresh_token):
        self.refresh_token = refresh_token

    def get_refresh_token(self):
        return self.refresh_token


class PollySession():
    """
    """

    configuration = None

    def __init__(self, **kwargs):
        if 'refresh_token' not in kwargs:
            raise MalFormedConfigurationException(
                "Refresh Token is required for login")
        self.configuration = SessionConfiguration(kwargs.pop('refresh_token'))

    def create_resource(self, resource_name):
        return RESORUCE_CLASS_MAPPING[resource_name](self.configuration)
