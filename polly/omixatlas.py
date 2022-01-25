import json
import logging
import pandas as pd
from retrying import retry

from polly.auth import Polly
from polly.errors import (
    QueryFailedException,
    UnfinishedQueryException,
    error_handler,
    is_unfinished_query_error,
    paramException, wrongParamException, apiErrorException
)


class OmixAtlas:

    def __init__(self, token=None, env="polly") -> None:
        self.session = Polly.get_session(token, env=env)
        self.base_url = f"https://v2.api.{self.session.env}.elucidata.io"
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

    

    def get_schema(self, repo_id: str, schema_type_dict: dict) -> dict:
        """
            Gets the schema of a repo id for the given repo_id and
            schema_type definition at the top level
            params:
            repo_id => str
            schema_type_dict => dictionary {schema_level:schema_type}

            example {'dataset': 'files', 'sample': 'gct_metadata'}


            Ouput:
                {
                    "data": {
                        "id": "<REPO_ID>",
                        "type": "schema",
                        "attributes": {
                            "schema_type": "files | gct_metadata | h5ad_metadata",
                            "schema": {
                                ... field definitions
                            }
                        }
                    }
                }
        """
        resp_dict = {}
        schema_base_url = f'{API_ENDPOINT}/repositories'
        if repo_id and schema_type_dict and isinstance(schema_type_dict, Dict):
            for key, val in schema_type_dict.items():
                schema_type = val
                dataset_url = f"{schema_base_url}/{repo_id}/schemas/{schema_type}"
                resp = self.session.get(dataset_url)
                error_handler(resp)
                resp_dict[key] = resp.json()
        else:
            raise paramException(
                title="Param Error",
                detail="repo_id and schema_type_dict are either empty or its datatype is not correct"
            )
        return resp_dict

    def visualize_schema(self, repo_id: str, schema_level=['dataset', 'sample'], single_cell=False) -> None:
        """
            Visualizing the schema of the repository depending on schema_type
            schema_type : gct_metadata or h5ad_metadata i.e Column Fields (Sample)
            metdata schema definition for sample:
                schema:{
                    "<SOURCE>": {
                        "<DATATYPE>": {
                            "<FIELD_NAME>": {
                            "original_name": "string" // this should match with source file/metadata
                            "type": "text | integer | object",
                            "is_keyword": boolean,
                            "is_array": boolean,
                            "is_filter": boolean,
                            "is_column": boolean,
                            "filter_size": integer, (Min=1, Max=3000, Default=500)
                            "display_name": "string", (Min=1, Max=30)
                            "description": "string", (Min=1, Max=100)
                            },
                            ... other fields
                        }
                        ... other Data types
                    }
                    ... other Sources
                }

            schema_type : files i.e Global Fields (dataset)
            metadata schema definition for a dataset:
                schema:{
                        "ALL": {
                            "ALL": {
                                "<FIELD_NAME>": {
                                "original_name": "string" // this should match with source file/metadata
                                "type": "text | integer | object",
                                "is_keyword": boolean,
                                "is_array": boolean,
                                "is_filter": boolean,
                                "is_column": boolean,
                                "filter_size": integer, (Min=1, Max=3000, Default=500)
                                "display_name": "string", (Min=1, Max=30)
                                "description": "string", (Min=1, Max=100)
                                },
                                ... other fields
                            }
                        }
            As Data Source and Data types segregation is not applicable
            at Dataset Level Information (applicable for sample metadata only)

            schema_type : gct_metadata i.e Row Fields (Feature)
            Not there right now
        """

        # get schema_type_dict
        schema_type_dict = self.get_schema_type(schema_level, single_cell)

        # schema from API calls
        if repo_id and schema_type_dict and isinstance(schema_type_dict, Dict):
            schema = self.get_schema(repo_id, schema_type_dict)

        if schema and isinstance(schema, Dict):
            for key, val in schema_type_dict.items():
                if 'dataset' in key and schema[key]['data']['attributes']['schema']:
                    schema[key] = schema[key]['data']['attributes']['schema']['all']['all']
                elif 'sample' in key and schema[key]['data']['attributes']['schema']:
                    schema[key] = schema[key]['data']['attributes']['schema']

        self.print_table(schema)

    def get_schema_type(self, schema_level: list, single_cell: bool) -> dict:
        """
            Compute schema_type based on repo_id and schema_level

            schema_level         schema_type
            ------------------------------------
            dataset         ==   file
            ----------------------------------
            sample          ==   gct_metadata
            -----------------------------------
            sample and      ==    h5ad_metadata
            single cell
        """
        if schema_level and isinstance(schema_level, list):
            if 'dataset' in schema_level and 'sample' in schema_level:
                if not single_cell:
                    schema_type_dict = {'dataset': 'files', 'sample': 'gct_metadata'}
                elif single_cell:
                    schema_type_dict = {'dataset': 'files', 'sample': 'h5ad_metadata'}
            elif 'dataset' in schema_level or 'sample' in schema_level:
                if 'dataset' in schema_level:
                    schema_type_dict = {'dataset': 'files'}
                elif 'sample' in schema_level:
                    if not single_cell:
                        schema_type_dict = {'sample': 'gct_metadata'}
                    elif single_cell:
                        schema_type_dict = {'sample': 'h5ad_metadata'}
            else:
                raise wrongParamException(
                    title="Incorrect Param Error",
                    detail="Incorrect value of param passed schema_level "
                )
        else:
            raise paramException(
                title="Param Error",
                detail="schema_level is either empty or its datatype is not correct"
            )
        return schema_type_dict

    def format_type(self, data: dict) -> dict:
        """
            Format the dict data
        """
        if data and isinstance(data, Dict):
            return json.dumps(data, indent=4)

    def print_table(self, schema_data: dict) -> None:
        """
            Print the Schema in a tabular format
        """
        global_fields = {}
        col_fields = {}
        if schema_data and isinstance(schema_data, Dict) and 'dataset' in schema_data:
            dataset_data = schema_data['dataset']
            global_fields = ''.join("\n '{name}': {type}".format(name=key,
                                    type=self.format_type(val)) for key, val in dataset_data.items())

        if schema_data and isinstance(schema_data, Dict) and 'sample' in schema_data:
            sample_data = schema_data['sample']
            col_fields = ''.join("\n '{name}': {type}".format(name=key,
                                 type=self.format_type(val)) for key, val in sample_data.items())

        if global_fields and col_fields:
            s = '----------------------------------------\n' \
                'Global fields(dataset):{g}\n' \
                '----------------------------------------\n' \
                'Column fields(Sample):{c}\n' \
                '----------------------------------------\n'.format(g=global_fields, c=col_fields)
        elif global_fields:
            s = '----------------------------------------\n' \
                'Global fields(dataset):{g}\n' \
                '----------------------------------------\n'.format(g=global_fields)
        elif col_fields:
            s = '----------------------------------------\n' \
                'Column fields(Sample):{c}\n' \
                '----------------------------------------\n'.format(c=col_fields)
        print(s)

    def insert_schema(self, repo_id: str, body: dict) -> dict:
        """
            Params:
                repo_id => str => ex:- "345652035432"
                body => dict
                {
                    "data": {
                        "id": "<REPO_ID>",
                        "type": "schema",
                        "attributes": {
                        "schema_type": "files | gct_metadata | h5ad_metadata",
                        "schema": {
                            ... field definitions
                        }
                        }
                    }
                }
        """
        if repo_id and body and isinstance(body, dict):
            body = json.dumps(body)
            try:
                schema_base_url = f'{API_ENDPOINT}/repositories'
                url = f"{schema_base_url}/{repo_id}/schemas"
                resp = self.session.post(url, data=body)
                error_handler(resp)
                return resp.text
            except Exception as err:
                raise apiErrorException(
                    title="API exception err",
                    detail=err
                )
        else:
            raise apiErrorException(
                title="Param Error",
                detail="Params are either empty or its datatype is not correct"
            )

<<<<<<< HEAD

>>>>>>> [WIP]:moving visualize feature to omixatlas class
=======
    def update_schema(self, repo_id: str, body: dict) -> dict:
        """
        Params:
                repo_id => str => ex:- "345652035432"
                body => dict
                {
                    "data": {
                        "id": "<REPO_ID>",
                        "type": "schema",
                        "attributes": {
                        "schema_type": "files | gct_metadata | h5ad_metadata",
                        "schema": {
                            ... field definitions
                        }
                        }
                    }
                }
        """
        schema_type = body['data']['attributes']['schema_type']
        print("------schema type-------")
        print(schema_type)
        schema_base_url = f'{API_ENDPOINT}/repositories'
        url = f"{schema_base_url}/{repo_id}/schemas/{schema_type}"
        print('----------url---------------')
        print(url)
        print('----type of payload----')
        print(type(body))
        if repo_id and body and isinstance(body, dict):
            body = json.dumps(body)
            try:
                resp = self.session.patch(url, data=body)
                print(resp)
                print(resp.content)
                return resp.text
            except Exception as err:
                raise apiErrorException(
                    title="API exception err",
                    detail=err
                )
        else:
            raise paramException(
                title="Param Error",
                detail="Params are either empty or its datatype is not correct"
            )


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

    def save_to_workspace(self, repo_id: str, dataset_id: str,
                          workspace_id: int,
                          workspace_path: str) -> json:
        '''
            Function for saving data from omixatlas to workspaces.
            Makes a call to v1/omixatlas/workspace_jobs
        '''
        url = f"{self.resource_url}/workspace_jobs"
        params = {"action": "copy"}
        payload = {
            "data": {
                "type": "workspaces",
                "attributes": {
                    "dataset_id": dataset_id,
                    "repo_id": repo_id,
                    "workspace_id": workspace_id,
                    "workspace_path": workspace_path
                }
            }
        }
        response = self.session.post(url,
                                     data=json.dumps(payload),
                                     params=params)
        error_handler(response)
        if response.status_code == 200:
            logging.basicConfig(level=logging.INFO)
            logging.info(f'Data Saved to workspace={workspace_id}')
        return response.json()


if __name__ == "__main__":
    client = OmixAtlas()
