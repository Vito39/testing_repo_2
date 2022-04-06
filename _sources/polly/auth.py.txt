from polly.session import PollySession

link_doc = "https://docs.elucidata.io/OmixAtlas/Polly%20Python.html"


class UnauthorizedException(Exception):
    def __str__(self):
        return f"Authorization failed as credentials not found. Please use Polly.auth(token) as shown here  ---- {link_doc}"


class Polly:
    """
    This class for authorization to use polly on local, which include following member functions.
    
    """
    default_session = None

    @classmethod
    def auth(cls, token, env="polly"):
        """
        Function for authorization to use polly on terminal or notebook.

        ``Args:``
            |  ``token (str):`` token copy from polly.
            |  ``env (str):`` polly(default) or testpolly or devpolly.

        
        ``Returns:``
            |  if token is not satisfied it will throw a error.
            |  else it will auth you.  
        
        ``Error:``
            |  ``UnauthorizedException:`` when the token is expired.

        To use auth function import class Polly.

        .. code::


                from polly.auth import Polly
                Polly.auth(token)
        """
        cls.default_session = PollySession(token, env=env)

    @classmethod
    def get_session(cls, token=None, env="polly"):
        """
        Function to get session from polly.

        ``Args:``
            |  ``token (str):`` token copy from polly.
            |  ``env (str):`` polly or testpolly or devpolly.

        ``Returns:``        
            |  if token is not satisfied it will throw UnauthorizedException.
            |  else it will return a polly.session object.
        
        ``Error:``
            |  ``UnauthorizedException:`` when the token is expired.

        To use get_seesion function import class Polly.


        .. code::


                from polly.auth import Polly
                session = Polly.get_session(token)

        """
        if not token:
            if not cls.default_session:
                raise UnauthorizedException
            else:
                return cls.default_session
        else:
            return PollySession(token, env=env)
