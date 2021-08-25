import pandas as pd
from polly.session import PollySession
from polly.errors import error_handler


class OmixAtlas:
    def __init__(self, token: str) -> None:
        self.session = PollySession(token)
        self.base_url = "https://v2.api.polly.elucidata.io/v1/omixatlases"

    def get_all_omixatlas(self):
        url = self.base_url
        params = {"summarize": "true"}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()

    def omixatlas_summary(self, key: str):
        url = f"{self.base_url}/{key}"
        params = {"summarize": "true"}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()

    def query_metadata(self, query: str, experimental_features=None):
        url = f"{self.base_url}/_query"
        payload = {"query": query}
        if experimental_features is not None:
            payload.update({"experimental_features": experimental_features})
        response = self.session.get(url, json=payload)
        error_handler(response)
        message = response.json().get('message', None)
        if message is not None:
            print(message)
        return self.__process_query_response(response.json())

    def __process_query_response(self, response: dict):
        # print(response)
        response.pop("took", None)
        response.pop("timed_out", None)
        response.pop("_shards", None)
        processed_response = None
        try:
            hits = response.get('hits').get('hits')
            if hits:
                processed_response = pd.DataFrame(hits)
            else:
                response.pop('hits', None)
                processed_response = response
        except AttributeError:
            processed_response = response
        return processed_response

    # ? DEPRECATED
    def search_metadata(self, query: dict):
        url = f"{self.base_url}/_search"
        payload = query
        response = self.session.get(url, json=payload)
        error_handler(response)
        return response.json()

    def download_data(self, repo_name, _id: str):
        url = f"{self.base_url}/{repo_name}/download"
        params = {"_id": _id}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()


if __name__ == "__main__":
    client = OmixAtlas()
