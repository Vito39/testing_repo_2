from requests import Session


class PollySession(Session):
    def __init__(self, REFRESH_TOKEN):
        Session.__init__(self)
        try:
            # for python version >= python3.8
            from importlib.metadata import version
            version = "polly-python/"+version('polly-python')
        except ImportError:
            # for python version < python3.8
            import pkg_resources
            version = pkg_resources.get_distribution('polly-python').version
        self.headers = {
            "Content-Type": "application/vnd.api+json",
            "Cookie": f"refreshToken={REFRESH_TOKEN}",
            "User-Agent": "polly-python/"+version,
        }
