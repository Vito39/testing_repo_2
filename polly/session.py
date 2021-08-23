from requests import Session
import pkg_resources

class PollySession(Session):
    def __init__(self, REFRESH_TOKEN):
        Session.__init__(self)
        version = pkg_resources.get_distribution('polly-python').version

        self.headers = {
            "Content-Type": "application/vnd.api+json",
            "Cookie" : f"refreshToken={REFRESH_TOKEN}",
            "User-Agent" : "polly-python/"+version,
        }
