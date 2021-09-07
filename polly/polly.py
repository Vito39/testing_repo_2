from polly.session import PollySession
link_doc = 'link to documentation'

class UnauthorizedException(Exception):
    def __str__(self):
        return f"Authorization failed as credentials not found. Please use Polly.auth(token) as shown here  ---- {link_doc}"


class Polly:
    default_session = None

    @classmethod
    def auth(cls, token):
        cls.default_session = PollySession(token)

    @classmethod
    def get_session(cls, token=None):
        if not token:
            if not cls.default_session:
                raise UnauthorizedException
            else:
                return cls.default_session
        else:
            return PollySession(token)
