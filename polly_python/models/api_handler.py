import requests


class ApiHandler():

    headers = {'Content-type': 'application/vnd.api+json'}

    def __init__(self):
        pass

    def send_request(self, method, **kwargs):
        if method == 'GET':
            return self._get(**kwargs)
        elif method == 'POST':
            return self._post(**kwargs)

    def _get(self, **kwargs):
        return requests.get()

    def _post(self):
        return requests.post()
    
    def _put(self):
        return requests.put()

    def _delete(self):
        return requests.delete()
