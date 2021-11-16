import pandas as pd
from retrying import retry

from polly.polly import Polly
from polly.errors import (
    QueryFailedException,
    UnfinishedQueryException,
    error_handler,
    is_unfinished_query_error
)


class OmixAtlas:
    def __init__(self, token=None, env="polly") -> None:
        self.session = Polly.get_session(token)
        self.base_url = f"https://v2.api.{env}.elucidata.io"
        self.resource_url = f"{self.base_url}/v1/omixatlases"

    def get_all_omixatlas(self):
        url = self.resource_url
        params = {"summarize": "true"}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()

    def omixatlas_summary(self, key: str):
        url = f"{self.resource_url}/{key}"
        params = {"summarize": "true"}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()

    def query_metadata(
        self,
        query: str,
        experimental_features=None,
        query_api_version="v2",
        page_size=500  # Note: do not increase page size more than 999
    ):
        max_page_size = 999
        if page_size > max_page_size:
            raise ValueError(
                f"The maximum permitted value for page_size is {max_page_size}"
            )

        queries_url = f"{self.resource_url}/queries"
        queries_payload = {
            "data": {
                "type": "queries",
                "attributes": {
                    "query": query,
                    "query_api_version": query_api_version
                }
            }
        }
        if experimental_features is not None:
            queries_payload.update({
                "experimental_features": experimental_features
            })

        response = self.session.post(queries_url, json=queries_payload)
        error_handler(response)

        query_data = response.json().get("data")
        query_id = query_data.get("id")
        return self._process_query_to_completion(query_id, page_size)

    @retry(
        retry_on_exception=is_unfinished_query_error,
        wait_exponential_multiplier=1000,  # Exponential back-off
        wait_exponential_max=10000,        # After 10s, retry every 10s
        stop_max_delay=300000              # Stop retrying after 300s (5m)
    )
    def _process_query_to_completion(self, query_id: str, page_size: int):
        queries_url = f"{self.resource_url}/queries/{query_id}"
        response = self.session.get(queries_url)
        error_handler(response)

        query_data = response.json().get("data")
        query_status = query_data.get("attributes", {}).get("status")
        if query_status == "succeeded":
            return self._handle_query_success(query_data, page_size)
        elif query_status == "failed":
            self._handle_query_failure(query_data)
        else:
            raise UnfinishedQueryException(query_id)

    def _handle_query_failure(self, query_data: dict):
        fail_msg = query_data.get("attributes").get("failure_reason")
        raise QueryFailedException(fail_msg)

    def _handle_query_success(
        self,
        query_data: dict,
        page_size: int
    ) -> pd.DataFrame:
        query_id = query_data.get("id")

        first_page_url = (
            f"{self.resource_url}/queries/{query_id}"
            f"/results?page[size]={page_size}"
        )
        response = self.session.get(first_page_url)
        error_handler(response)
        result_data = response.json()
        rows = [
            row_data.get("attributes") for row_data in result_data.get("data")
        ]

        all_rows = rows

        message = "Fetched {} rows"
        print(message.format(len(all_rows)), end="\r")

        while (
            result_data.get("links") is not None
            and result_data.get("links").get("next") is not None
            and result_data.get("links").get("next") != "null"
        ):
            next_page_url = self.base_url + result_data.get("links").get("next")
            response = self.session.get(next_page_url)
            error_handler(response)
            result_data = response.json()
            if result_data.get("data"):
                rows = [
                    row_data.get("attributes")
                    for row_data in result_data.get("data")
                ]
            else:
                rows = []
            all_rows.extend(rows)
            print(message.format(len(all_rows)), end="\r")

        # Blank line resets console line start position
        print()

        return pd.DataFrame(all_rows)

    # ? DEPRECATED
    def search_metadata(self, query: dict):
        url = f"{self.resource_url}/_search"
        payload = query
        response = self.session.get(url, json=payload)
        error_handler(response)
        return response.json()

    def download_data(self, repo_name, _id: str):
        url = f"{self.resource_url}/{repo_name}/download"
        params = {"_id": _id}
        response = self.session.get(url, params=params)
        error_handler(response)
        return response.json()


if __name__ == "__main__":
    client = OmixAtlas()
