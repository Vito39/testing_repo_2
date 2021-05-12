import configparser
import os
import getpass
from pycognito import Cognito

from polly_python.constants import USER_PROFILE_PATH, COGNITO_USER_POOL, COGNITO_CLIENT_ID


class PollyLogin():

    cognito_user = None

    def __init__(self):
        user_email = input("Enter your polly email address: ")
        passwd = getpass.getpass('Enter your password: ')
        self.cognito_user = Cognito(COGNITO_USER_POOL,
                                    COGNITO_CLIENT_ID,
                                    username=user_email)
        self.cognito_user.authenticate(password=passwd)
        self._login()

    def _login(self):
        config = configparser.ConfigParser()
        config['default'] = {
            'refresh_token': self.cognito_user.refresh_token,
            'id_token': self.cognito_user.id_token,
            'username': self.cognito_user.username
        }
        self._clean_folder()
        with open(USER_PROFILE_PATH, 'w') as configfile:
            config.write(configfile)

    def _clean_folder(self):
        if os.path.exists(USER_PROFILE_PATH):
            os.remove(USER_PROFILE_PATH)


class PollyLogout():
    def __init__(self):
        self._logout()

    def _logout(self):
        if os.path.exists(USER_PROFILE_PATH):
            os.remove(USER_PROFILE_PATH)
