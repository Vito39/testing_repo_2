import requests
from polly_python.exceptions import MethodNotAllowedException


class ApiHandler():

    headers = {'Content-type': 'application/vnd.api+json'}

    def __init__(self):
        pass

    def dispatch_request(self, method, session, **kwargs):
        return self._dispatch_request(method, session, **kwargs)

    def get_cookies(self, session):
        cookies = {}
        if session.get_refresh_token():
            cookies['polly.refreshToken'] = session.get_refresh_token()
        if session.get_id_token():
            cookies['polly.idToken'] = session.get_id_token()
        return cookies

    def _dispatch_request(self, method, session, **kwargs):
        # TODO Simplify
        if method == 'GET':
            return self._get(session, **kwargs)
        elif method == 'POST':
            return self._post(**kwargs)
        elif method == 'PUT':
            return self._put(**kwargs)
        elif method == 'DELETE':
            return self._delete(**kwargs)
        else:
            raise MethodNotAllowedException()

    def _get(self, session, **kwargs):
        response = requests.get(url=kwargs.get('url'),
                                cookies=self.get_cookies(session),
                                headers=self.headers,
                                params=kwargs.get('params'))
        return self._process_response(response, session)

    def _post(self):
        return requests.post()

    def _put(self):
        return requests.put()

    def _delete(self):
        return requests.delete()

    def _process_response(self, response, session):

        cookie_dict = response.cookies.get_dict(domain='.polly.elucidata.io')

        if cookie_dict.get('polly.idToken') and session.get_id_token(
        ) != cookie_dict.get('polly.idToken'):
            session.set_id_token(cookie_dict['polly.idToken'])

        return response
