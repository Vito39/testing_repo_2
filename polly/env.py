# import os
# import re

# from polly.helpers import make_discover_request

# def __get_discover_api_url():
#     env = get_curr_exec_env()
#     discover_api_base_urls = {
#         "dev": "https://api.discover.devpolly.elucidata.io",
#         "test": "https://api.discover.testpolly.elucidata.io",
#         "prod": "https://api.discover.polly.elucidata.io",
#         "eupolly": "https://api.discover.eu-polly.elucidata.io"
#     }
#     return discover_api_base_urls.get(env)


# def get_curr_exec_env():
#     env = "dev"
#     if not env:
#         POLLY_API_URL = os.getenv('POLLY_TYPE')
#         if POLLY_API_URL:
#             env = re.search('https://(.*)polly.elucidata.io',
#                             POLLY_API_URL).group(1)
#             if not env:
#                 env = "prod"
#         else:
#             raise Exception('Unable to set identify execution env')
#     return env


# try:
#     DISCOVER_API_URL = __get_discover_api_url()
#     CONSTANTS_URL = DISCOVER_API_URL+'/constants'
#     resp = make_discover_request('GET', CONSTANTS_URL)
#     CONSTANTS = resp.json()['data']['attributes']
#     ELASTIC_DOMAIN_URL = CONSTANTS['ELASTIC_DOMAIN_URL']
#     S3_DATALAKE_BUCKET = CONSTANTS['S3_DATALAKE_BUCKET']
#     AWS_REGION = CONSTANTS['AWS_REGION']
#     TOKENS_URL = CONSTANTS['REPOSITORY_TOKENS_URL']
# except Exception:
#     raise Exception('Unable to set Discover Configuration. '
#                     'Contact Administrator')
