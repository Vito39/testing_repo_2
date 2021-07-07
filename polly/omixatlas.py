import requests
import pandas as pd

from polly.errors import error_handler


class OmixAtlas:
    def __init__(self, refresh_token: str) -> None:
        self.base_url = "https://v2.api.testpolly.elucidata.io/v1/omixatlases"
        self.headers = {
            "Content-Type": "application/vnd.api+json",
            "Cookie": f"refreshToken={refresh_token}",
        }

    def get_all_omixatlas(self):
        url = self.base_url
        params = {"summarize": "true"}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def omixatlas_summary(self, key: str):
        url = f"{self.base_url}/{key}"
        params = {"summarize": "true"}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def query_metadata(self, query: str):
        url = f"{self.base_url}/_query"
        payload = {"query": query}
        response = requests.get(url, headers=self.headers, json=payload)
        error_handler(response)
        try:
            hits = response.json().get("hits").get("hits")
            hits_dataframe = pd.DataFrame(hits)
            return hits_dataframe
        except AttributeError:
            return response.json()

    # ? DEPRECATED
    def search_metadata(self, query: dict):
        url = f"{self.base_url}/_search"
        payload = query
        response = requests.get(url, headers=self.headers, json=payload)
        error_handler(response)
        return response.json()

    def download_data(self, repo_name, _id: str):
        url = f"{self.base_url}/{repo_name}/download"
        params = {"_id": _id}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
