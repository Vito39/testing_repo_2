import os
import pickle

from polly_python.models.user import User
from polly_python.api_handler import ApiHandler
from polly_python.constants import SESSION_FOLDER, V2_API_ENDPOINT, SESSION_OBJECT_PATH


class Polly():
    """
    Stores your credentials to interact
    with Elucidata's Polly Infrastructure.
    """

    polly_session_manager = None

    def __init__(self, refresh_token: str):
        self.polly_session_manager = PollySessionManager(refresh_token)

    def get_polly_session_manager(self):
        return self.polly_session_manager


class PollySessionManager():

    refresh_token = None
    id_token = None
    user = None

    def __init__(self, refresh_token):
        self.refresh_token = refresh_token

        if not os.path.exists(SESSION_FOLDER):
            os.makedirs(SESSION_FOLDER)

        self._save_session()
        pass

    def set_id_token(self, id_token):
        self.id_token = id_token
        self._save_session()

    def get_id_token(self):
        return self.id_token

    def get_refresh_token(self):
        return self.refresh_token

    def _save_session(self):
        if self.id_token is None:
            self.__init_user()
        self._save()

    def _save(self):
        """Saves the object as pickled session
        need to add checks for directory and other
        chmod permission, also for check for windows os path
        """
        if os.path.exists(SESSION_OBJECT_PATH):
            os.remove(SESSION_OBJECT_PATH)
        with open(SESSION_OBJECT_PATH, 'wb') as fp:
            pickle.dump(self, fp)

    def __init_user(self):
        api_handler = ApiHandler()
        response = api_handler.dispatch_request('GET',
                                                self,
                                                url=V2_API_ENDPOINT +
                                                '/users/me')
        if response.status_code != 200:
            raise Exception('User authentication Failure.')

        self.user = User.from_api_response(response)
