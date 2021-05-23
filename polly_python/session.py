import os
import pickle

from polly_python.constants import SESSION_OBJECT_PATH, SESSION_FOLDER


class SessionConfiguration():
    """
    Stores your credentials to interact
    with Elucidata's Polly Infrastructure.
    """

    refresh_token: str = None

    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        if not os.path.exists(SESSION_FOLDER):
            os.makedirs(SESSION_FOLDER)

    def get_refresh_token(self):
        return self.refresh_token

    def save(self):

        if os.path.exists(SESSION_OBJECT_PATH):
            print('Session object already present, recreating....')
            os.remove(SESSION_OBJECT_PATH)

        with open(SESSION_OBJECT_PATH, 'wb') as session_file:
            pickle.dump(self, session_file, pickle.HIGHEST_PROTOCOL)
