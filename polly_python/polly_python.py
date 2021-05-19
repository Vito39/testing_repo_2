"""Main module."""
from polly_python.session import PollySession

DEFAULT_SESSION = None


def init(**kwargs):
    return _setup_default_session(**kwargs)


def _setup_default_session(**kwargs):
    global DEFAULT_SESSION
    DEFAULT_SESSION = PollySession(**kwargs)
    return DEFAULT_SESSION
