import json
from polly.auth import Polly
from polly.errors import error_handler
from polly.constants import API_ENDPOINT
from typing import Dict


class SchemaVisualization(object):
    def __init__(self, token=None) -> None:
        self.session = Polly.get_session(token)
        # self.base_url = f'{V2_API_ENDPOINT}/v1/omixatlases'
        self.base_url = f'{API_ENDPOINT}/repositories'
    

    def get_schema(self, repo_id:str, schema_type:str)-> dict:
        """
            Gets the schema of a repo id for the given repo_id and 
            schema_type
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
        if repo_id and schema_type:
            url = f"{self.base_url}/{repo_id}/schemas/{schema_type}"
        print(url)
        # params = {"schema_type": schema_type}
        response = self.session.get(url)
        # print(response)

        return response


    
    def visualize(self, schema: dict) -> None:
        """
            Visualizing the schema of the repository depending on schema_type
            schema:
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
                
            schema_type : files i.e Global Fields (dataset)
            schema_type : gct_metadata i.e Column Fields (Sample)
            schema_type : gct_metadata i.e Row Fields (Feature)
        """
        if schema and isinstance(schema, Dict):
            if schema['data']['attributes']['schema_type']:
                schema_type = schema['data']['attributes']['schema_type']
            if schema['data']['attributes']['schema']:
                schema_data = schema['data']['attributes']['schema']
        print(schema_type)
        self.print_table(schema_type, schema_data)


    
    def format_type(self, data: dict)-> dict:
        """
            Format the dict data
        """
        if data and isinstance(data, Dict):
            return json.dumps(data, indent=4)
    
    
    
    def print_table(self, schema_type: str, schema_data: dict) ->None:
        """
            Print the Schema in a tabular format
        """
        
        if schema_type and schema_type == 'files':
            if schema_data and isinstance(schema_data, Dict):
                global_fields = ''.join("\n    '{name}': {type}".format(
                name=key, type = self.format_type(val)) for key, val in schema_data.items())
            row_fields = '\n    None'
            col_fields = '\n    None'

        elif schema_type and schema_type == 'gct_metadata' or 'h5ad_metadata':
            if schema_data and isinstance(schema_data, Dict):
                col_fields = ''.join("\n    '{name}': {type}".format(
                name=key, type = self.format_type(val)) for key, val in schema_data.items())
            global_fields = '\n    None'
            row_fields = '\n    None'
        
        elif schema_type and schema_type == 'gct_row_metadata':
            if schema_data and isinstance(schema_data, Dict):
                row_fields = ''.join("\n    '{name}': {type}".format(
                name=key, type = self.format_type(val)) for key, val in schema_data.items())
            global_fields = '\n    None'
            col_fields = '\n    None'
        
        s = '----------------------------------------\n' \
            'Global fields(dataset):{g}\n' \
            '----------------------------------------\n' \
            'Column fields(Sample):{c}\n' \
            '----------------------------------------\n' \
            'Row fields(Feature):{r}\n' \
            '----------------------------------------\n'.format(g=global_fields,
                                                              r=row_fields,
                                                              c=col_fields)
        print(s)


# token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.G8ZXMcoHzj3nI1nglOFm2i1q8n-Zfb9gl5krrHpQG3bSO8BpPSd_P--nZdr3dGrOAB-zMEARd1J8FTBx4GSom3Wb5M6pxSnj3FMZIt3iJyTlC71qKdlGgtWuOPAv0S7UafEGxlKRJ-YiP6bu0Q9d7oLMYkTxfoQjPEV5bHcs4KP9KzZ6O34RMBVz0QQY_6UuXfkAi7MeSRggI2sFuX3JB04ZYajk-aoh5tdzh2iFPpccrYO6KLAhGYVvUCj6Dk5DgGQrO-oR4Qq9MxHLyGX5NCq8GQzpKkd3yEPSq_s2dB6Rv27S5GB9WXax9fsy65Ajh6kaZ52ES0mktvQxOC7CkQ.YcOjxvMlYke5vcjR.r_nyouv2eobL0OxXWziT2V0H6e--clybZsU82CZxxrRPFoTp3XV8kMT69Cjee4RY8IThIqgWXj0W1X8bhZfprTgoRY7hfrKzxgczBeSixH9i6CwhSULiXV6swIPvHqraGCGs-D0lfS6GbuHJ3kHdX_FmSJEzszst5blQy4agTZgE8EYO9QHbCip5i88hBmvL4aUmfRfZ1KwUOtgXbeaigR95oWBqFs7pOYRgzTwdUD18xqQXCuH18AsJo_40sO62joMijKP1oY5DjHt3MHk5Ou-rMfpGolOMno1K5mHsHd3ZTolfbw02t4tedDXCXGml1d9xmDNK3bTo-Wyjp_8fuIzBW5g6IQCvOarstvm0jKELWnERf9QekenmAm6z2K37uKzickG7YpnYvwgVjghn_AhaK6mLzEoNuvAyQlqkm6XhdktV13AgFEa43RjdXZkIaob41dpa_HeqF27xKaMNYI68IGDNuZd_CJzmQmUQdp5-_SIXlNElr6OKKvtd2YC9e3HqsNPexcs-KKrs6HTLNWI955ZA-d5b4gBdcv4u_3oGpapjLlh476b_sVywRaDX-87kkKvkqaFMcK2CrzCI3lQaHJLioFnTT_E647vYcrfDUCXbyGww52HGkD7YPdVSoEXXyZQg-2pkfBc9GryJ814kD60-2gA1XecqQVFG6bBRzbzxG42zjfI4GQcntOUhYG1LY6WYIUVaZKgUqaORzsicG-A_m0cujw0285AsyCNQPkoPyuZzlZxzUqfxsbps5WEvTCTdKVXV9Hi0exnxhOw6TCwqDDtlNx9WyORCNHCDUvh2Sb5fRvPxTLn1MJ5qADbLDS4FVX5g5hC1jbxZfPT-J647LCtkB_wr_S40pGmOoWitBbz0_kEZem_hXJgv2sHYJcYKtllXwJTbsOBZHLmNzgv4GkbX0zRPghFDL23UkyZFWpiEPoEhzYUHzH3n-7xVZM5yGGX87bgPVfDG8foGzcy76zjZPJf1kySMU4PZQSLdAgLzwcSXphkQDGR5GuhTI3GbJig4jXf6pjGqIEUEzDgRK5IlVXhhPrin6Zca-66XDhfPA2KSXj_QVms189oYPIeia27vdzizGEuweXdBeFNssRtAkUZsYnK2ZSpFUfAygi64onNnwTlSjChVStkft5yG5D6FHQdiSTALZ4eT5AThG7YktNPKSOeGtdLAXJU4qTiSCTrOnqtV7XDMsSsDR6EQyDPUzlEEsv5kR2R6ZB8yHbqLQE4PxB5YxEDjhjywcO9VFurFDVoBu2k7hZ12E4K3WvX-Kjy4fPLbSkcY_cM2ZI3XrIG_SIvY4DQMRL0QxVTeXa47f9vLjQc7z_EtiO2or-xz.15oKPoz2WqWKCaBhEY7Umw"

# schema_viz_obj = SchemaVisualization(token)

# resp = schema_viz_obj.get_schema("1622526550765", "gct_metadata")

# print('------resp--------')
# print(resp)


