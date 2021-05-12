"""Main module."""
from polly_python.exceptions import MalFormedConfigurationException
from polly_python.session import PollySession

DEFAULT_SESSION = None

def init(**kwargs):
    return setup_default_session()

def setup_default_session():
    """
    Creates default session for polly python sdk
    """
    try:
        global DEFAULT_SESSION
        DEFAULT_SESSION = PollySession()
        return DEFAULT_SESSION
    except MalFormedConfigurationException:
        pass

