import requests
from polly_python.exceptions import MethodNotAllowedException


class ApiHandler():

    headers = {'Content-type': 'application/vnd.api+json'}
    session = None

    def __init__(self, session = None):
        self.session = session
        pass

    def dispatch_request(self, method, session = None, **kwargs):
        if session is None and self.session is None:
            raise Exception('Session Not Available')
        self.session = session
        return self._dispatch_request(method, **kwargs)

    def _dispatch_request(self, method, **kwargs):
        if method == 'GET':
            return self._get(
                {'polly.refreshToken': self.session.get_refresh_token()}, **kwargs)
        elif method == 'POST':
            return self._post(**kwargs)
        elif method == 'PUT':
            return self._put(**kwargs)
        elif method == 'DELETE':
            return self._delete(**kwargs)
        else:
            raise MethodNotAllowedException()

    def _get(self, cookies, **kwargs):
        return requests.get(url=kwargs.get('url'),
                            cookies=cookies,
                            headers=self.headers,
                            params=kwargs.get('params'))

    def _post(self):
        return requests.post()

    def _put(self):
        return requests.put()

    def _delete(self):
        return requests.delete()
