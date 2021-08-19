from requests import Session

class PollySession(Session):
    def __init__(self, REFRESH_TOKEN):
        Session.__init__(self)

        self.headers = {
            "Content-Type": "application/vnd.api+json",
            "Cookie" : f"refreshToken={REFRESH_TOKEN}",
            "User-Agent" : "polly-python/0.0.5",
        }