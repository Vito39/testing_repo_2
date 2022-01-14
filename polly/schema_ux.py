import json
from polly.auth import Polly
from polly.errors import error_handler, paramException, wrongParamException
from polly.constants import API_ENDPOINT
from typing import Dict


class SchemaVisualization(object):
    def __init__(self, token=None) -> None:
        self.session = Polly.get_session(token)
        self.base_url = f'{API_ENDPOINT}/repositories'
    

    def get_schema(self, repo_id:str, schema_type_dict: dict)-> dict:
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
        if repo_id and schema_type_dict and isinstance(schema_type_dict, Dict):
            for key,val in schema_type_dict.items():
                schema_type = val
                dataset_url = f"{self.base_url}/{repo_id}/schemas/{schema_type}"
                resp = self.session.get(dataset_url)
                resp_dict[key] = resp.json()
        else:
            raise paramException(
                title="Param Error",
                detail=f"{repo_id} and {schema_type_dict} are either empty or its datatype is not correct"
            )
        return resp_dict

    
    def visualize(self, repo_id:str, schema_level=['dataset', 'sample'], single_cell=False)-> None:
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
            As Data Source and Data types segregation is not applicable at Dataset Level Information (applicable for sample metadata only)
            
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



    def get_schema_type(self, schema_level: list, single_cell:bool) -> dict:
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
                    schema_type_dict = {'dataset':'files', 'sample':'gct_metadata'}
                elif single_cell:
                    schema_type_dict = {'dataset':'files', 'sample':'h5ad_metadata'}
            elif 'dataset' in schema_level or 'sample' in schema_level:
                if 'dataset' in schema_level:
                    schema_type_dict = {'dataset':'files'}
                elif 'sample'in schema_level:
                    if not single_cell:
                        schema_type_dict = {'sample':'gct_metadata'}
                    elif single_cell:
                        schema_type_dict = {'sample':'h5ad_metadata'}     
            else:
                raise wrongParamException(
                    title="Incorrect Param Error",
                    detail=f"Incorrect value of param passed {schema_level} "
                )      
        else:
            raise paramException(
                title="Param Error",
                detail=f"{schema_level} is either empty or its datatype is not correct"
            )
        return schema_type_dict

    
    def format_type(self, data: dict)-> dict:
        """
            Format the dict data
        """
        if data and isinstance(data, Dict):
            return json.dumps(data, indent=4)
    
    
    
    def print_table(self, schema_data: dict)->None:
        """
            Print the Schema in a tabular format
        """
        global_fields = {}
        col_fields = {}
        if schema_data and isinstance(schema_data, Dict) and 'dataset' in schema_data:
            dataset_data = schema_data['dataset']
            global_fields = ''.join("\n    '{name}': {type}".format(
            name=key, type = self.format_type(val)) for key, val in dataset_data.items())

        
        if schema_data and isinstance(schema_data, Dict) and 'sample' in schema_data:
            sample_data = schema_data['sample']
            col_fields = ''.join("\n    '{name}': {type}".format(
            name=key, type = self.format_type(val)) for key, val in sample_data.items())


        if global_fields and col_fields:
            s = '----------------------------------------\n' \
            'Global fields(dataset):{g}\n' \
            '----------------------------------------\n' \
            'Column fields(Sample):{c}\n' \
            '----------------------------------------\n'.format(g=global_fields,
                                                              c=col_fields)
        elif global_fields:
            s = '----------------------------------------\n' \
            'Global fields(dataset):{g}\n' \
            '----------------------------------------\n'.format(g=global_fields)
        elif col_fields:
            s = '----------------------------------------\n' \
            'Column fields(Sample):{c}\n' \
            '----------------------------------------\n'.format(g=global_fields,
                                                              c=col_fields)
        print(s)



