from polly.session import PollySession


class UnauthorizedException(Exception):
    def __str__(self):
        return "Authorization failed as credentials not found. Please try again."


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
