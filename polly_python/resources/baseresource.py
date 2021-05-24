import pickle

from polly_python.constants import SESSION_OBJECT_PATH


class BaseResource():

    session = None

    def __init__(self):
        if self.session is None:
            self._load_session()

    def _load_session(self):
        with open(SESSION_OBJECT_PATH, 'rb') as session_file:
            self.session = pickle.load(session_file)
