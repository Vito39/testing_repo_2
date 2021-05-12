import os.path

import configparser

from polly_python import constants
from polly_python.exceptions import MalFormedConfigurationException
from polly_python.models.user import User


class PollySession():
    """
    """

    user = None
    id_token = None
    refresh_token = None

    def __init__(self):
        config = configparser.ConfigParser()

        if not os.path.isfile(constants.USER_PROFILE_PATH):
            raise MalFormedConfigurationException("Config file not found")

        config.read(constants.USER_PROFILE_PATH)

        if 'default' not in config:
            raise MalFormedConfigurationException(
                "No profile default present in the config")

        if 'refresh_token' not in config['default']:
            raise MalFormedConfigurationException("No Refresh Token provided")

        self.refresh_token = config['default']['refresh_token']

        if config.has_option('default', 'id_token'):
            self.id_token = config['default']['id_token']

        self.user = User(self.refresh_token)

    def get_user(self):
        return self.user

    def get_id_token(self):
        return self.id_token
