import requests


class ApiHandler():

    method = None
    body = None
    params = None
    cookies = None
    url = None
    headers = {'Content-type': 'application/vnd.api+json'}

    def __init__(self, method, url, body = None, params = None, cookies = None):
        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise Exception("Invalid Request method")

        self.method = method
        self.body = body
        self.params = params
        self.cookies = cookies
        self.url = url

    def query(self):
        return requests.request(self.method,
                                url=self.url,
                                params=self.params,
                                data=self.body,
                                headers=self.headers,
                                cookies=self.cookies)
