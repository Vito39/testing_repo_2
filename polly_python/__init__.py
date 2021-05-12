from polly_python.exceptions import MalFormedConfigurationException
from polly_python.session import PollySession

"""Top-level package for polly_python."""

__author__ = """ElucidataInc"""
__email__ = 'kunal.sharma@elucidata.io'
__version__ = '0.0.1'

DEFAULT_SESSION = None


def setup_default_session():
    """
    Creates default session for polly python sdk
    """
    try:
        global DEFAULT_SESSION
        DEFAULT_SESSION = PollySession()
    except MalFormedConfigurationException:
        pass


def _get_default_session():
    """
    Get the default polly session, creating one if needed.
    """
    if DEFAULT_SESSION is None:
        setup_default_session()

    return DEFAULT_SESSION

setup_default_session()
