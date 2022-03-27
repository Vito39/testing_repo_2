import os
import json
import logging
import urllib.request
from cloudpathlib import S3Client
from botocore.exceptions import ClientError
from cmapPy.pandasGEXpress.parse_gct import parse
from polly.errors import (
    error_handler,
    InvalidParameterException,
    MissingKeyException,
    InvalidPathException,
    OperationFailedException,
    paramException,
)

devpolly_token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.TRzwIPGWSPqP6dr1Pu3J0AgHbmb9u7cmg-6bSSo-8LTZE9nv_c9G5YDEFCqKagNRfd4OxkVp3lH50LUEN25jQnmpn6TQwOQfVJUH5CF-Z70B2uZaAAYicFi_nfjltZR6sgf8xd6a5StjALyGwidU97o8_yBOfPipxW6hY-3D94MgceL8AjLW3GhHtX2eda3h2uFVb1-NCSFcJIzKjy_a-Zrn9yiOPog57-nyG79TOZamyGQ4zxJrOUTVFAnnlhwYkBtDlUBl5KkZVNNUa5OKauwxKRwkjCNc2IaqQRlA6Hj_rNAeEUaGZIhRelZYWj9RoKFBx91GoG7z72JUvJiScg.KWS1Z1G8uQUnOGlR.6knmdcKmV9qRDM1lwpIWQcFwpCM2pDAl9kKODS-jtoUGZAP2ONB9BDwdgYJIv0LfwVBix8dAAamar3jAh0_UDT-A75YSX0QzfTsEPwl2qPCTQpvt8oDYU_1b1GCkeUMNhMCj18hJ0X_q3nyqVAy_ZEoZQCb4LAuqWrts8vNY6SIideeXeStYGEcJnXWAVf5wvWUuiVgLVa5m3dX53D9jAGzltM0HqazTL7jpXO0G5AFaFziPbXBcM0quaV6vS5S96d3iAp-OFud5YzH4u7rl3eC8X_KsDLlcuJ8SQMtBMIXZlVSq-eKvL0kc_5ugOXB19JAKlV6Ie4D8s7X7N33LxO0TF4hc6BVBdV4eLmhdF59teZTSHB2dzqVl_0nCzjXArJs1Yp4SUTpqdXpQK4w_7PPUwSUrZt4EY93PQBO4Pi6FDStsvchZqqcAAUVAkPliT-PVTh6wnxI--XO71criYtQByeyn1rRJ5NZ_LaYA9sbLvv46-O7T7o5zjYjZNMvINP6yVM6xYno6EeVfcVivk6PeJ1rAk2ihwjCAH-j8QZ-M-is6lKYeaKyT63Ol9H8Br9zF_26Nxpk5phSJ2Z93KWwXKjGF4Il6TFuX5MC9gfC4_41UDSYHqgs4c3TB1XIWQb6q8MlD7M166GOWIrMVA-1DCb-4VtyPMTHfUpnQOf-Jny8uVhsec8Vda2V2d-zdlOKB_wrGuAkrMsasfSvntR2kalJRTYiYV4aBJeTS1sB4_2rkI0qVe1z4Re5yCfCxUcQITijvrkKX10rQl_2-y1K7fGHTlzrIKATgnfX9R6t0y-rGcA2Ojl_XNGFS1mu499KeZpz35oSnwOl5Ixp6KYmMvA1_Gkk1zkr_xNhCGbTH3Q1NVtWZzsPiH21y5ixBFa_NXIbNXZiPHgDKD7Vivu7yikikLKxHVxZ8juNiZB3TEvxksqfegMUbRwnSWwiON5u1pCy2dYo_8PMzqpaz9KF5rWK57_oC349GiuVHo03yZbEsTxM2SP_yxfnzTdu5UQnyRjBAK3wEOosF80h5QDfSiQCE9De-3u0RC3D9R6q469AI_Z_-Pde1qLEoDbVyXf2wVibDIuEUwP3wE5kSwd5t6iDyBHz82KGxH4O8CsP9EIOMwI5ft_H4DfSMQUCWE6WTWKmJH-Wmnkd3IizxOkiAl9EBRikxJBzZKN8WUkGyWjenrq5VOpqio7Byz-O4ZZW78j409mlHJ2SxBck7xn8BATZ6fRy6dHS1ndsyHGOZYdlYhZ7KFZpNWG2FLBQlGS-q-v1p_yofb1drs_bffdM72qznYqp4XTomJosDb3s2zRwL3DFE_p3O6O7MLWqgi3jX7EPYydu2w7EYcQw.4PbeKTbr7ES_c4n7t76EgA"



def make_path(prefix: any, postfix: any) -> str:
    """
    Function to make and return a valid path
    """
    if not prefix:
        raise InvalidParameterException("prefix")
    if not postfix:
        raise InvalidParameterException("postfix")
    return os.path.normpath(f"{prefix}/{postfix}")


def get_sts_creds(sts_dict: dict) -> dict:
    """
    Function to check and return temporary sts creds
    """
    if sts_dict and isinstance(sts_dict, dict):
        if "data" in sts_dict:
            data = sts_dict.get("data")
            if "attributes" in data[0]:
                attributes = data[0].get("attributes")
                if "credentials" in attributes:
                    return attributes.get("credentials")
                else:
                    raise MissingKeyException("credentials")
            else:
                raise MissingKeyException("attributes")
        else:
            raise MissingKeyException("data")
    else:
        raise InvalidParameterException("sts_dict")


def upload_to_S3(cloud_path: str, local_path: str, credentials: dict) -> None:
    """
    Function to upload file/folder to S3 cloud path
    """
    access_key_id = credentials["AccessKeyId"]
    secret_access_key = credentials["SecretAccessKey"]
    session_token = credentials["SessionToken"]
    client = S3Client(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
    )
    source_path = client.CloudPath(cloud_path)
    if not source_path.exists():
        source_path.mkdir()
    try:
        source_path.upload_from(local_path, force_overwrite_to_cloud=True)
    except ClientError as e:
        raise OperationFailedException(e)


def download_from_S3(cloud_path: str, workspace_path: str, credentials: dict) -> None:
    """
    Function to download file/folder from workspaces
    """
    access_key_id = credentials["AccessKeyId"]
    secret_access_key = credentials["SecretAccessKey"]
    session_token = credentials["SessionToken"]
    client = S3Client(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
    )
    source_path = client.CloudPath(cloud_path)
    if not source_path.exists():
        raise InvalidPathException
    isFile = source_path.is_file()
    if isFile:
        try:
            dest_path = os.getcwd()
            source_path.copy(dest_path, force_overwrite_to_cloud=True)
            logging.basicConfig(level=logging.INFO)
            logging.info(f"Download successful to path={dest_path}")
        except ClientError as e:
            raise OperationFailedException(e)
    else:
        if not cloud_path.endswith("/"):
            cloud_path += "/"
        source_path = client.CloudPath(cloud_path)
        if not source_path.is_dir():
            raise InvalidPathException
        try:
            dest_path = f"{make_path(os.getcwd(),workspace_path)}"
            source_path.copytree(dest_path, force_overwrite_to_cloud=True)
            logging.basicConfig(level=logging.INFO)
            logging.info(f"Download successful to path={dest_path}")
        except ClientError as e:
            raise OperationFailedException(e)


def file_conversion(
    self, repo_info: str, dataset_id: str, format: str, header_mapping: dict
) -> None:
    """
    Function that converts file to mentioned format
    """
    if not (repo_info and isinstance(repo_info, str)):
        raise InvalidParameterException("repo_name/repo_id")
    if not (dataset_id and isinstance(dataset_id, str)):
        raise InvalidParameterException("dataset_id")
    if not (format and isinstance(format, str)):
        raise InvalidParameterException("format")
    if not isinstance(header_mapping, dict):
        raise InvalidParameterException("header_mapping")
    download_dict = self.download_data(repo_info, dataset_id)
    url = download_dict.get("data", {}).get("attributes", {}).get("download_url")
    if not url:
        raise MissingKeyException("dataset url")
    file_name = f"{dataset_id}.gct"
    try:
        urllib.request.urlretrieve(url, file_name)
        data = parse(file_name)
        os.remove(file_name)
        row_metadata = data.row_metadata_df
        if header_mapping:
            row_metadata = row_metadata.rename(header_mapping, axis=1)
        row_metadata.to_csv(f"{dataset_id}.{format}", sep="\t")
    except Exception as e:
        raise OperationFailedException(e)


def get_data_type(self, url: str, payload: dict) -> str:
    """
    Function to return the data-type of the required dataset
    """
    if not (url and isinstance(url, str)):
        raise InvalidParameterException("url")
    if not (payload and isinstance(payload, dict)):
        raise InvalidParameterException("payload")
    response = self.session.post(url, data=json.dumps(payload))
    error_handler(response)
    response_data = response.json()
    hits = response_data.get("hits", {}).get("hits")
    if not (hits and isinstance(hits, list)):
        raise paramException(
            title="Param Error",
            detail="No matches found with the given repo details. Please try again.",
        )
    dataset = hits[0]
    data_type = dataset.get("_source", {}).get("kw_data_type")
    if not data_type:
        raise MissingKeyException("data_type")
    return data_type

def make_discover_request(method, url, **kwargs):
    """ 
        Call requests.request() with headers required for authentication and
        json config.
        params:
            method: HttpMethod eg., GET, POST, etc..
            url:    httpUrl
            kwargs: keyword arguments supported by requests.request
        response:
            <requests.models.Response> object
    """
    import requests

    if not kwargs.get('headers'):
        kwargs['headers'] = {}
    kwargs['headers']['Content-Type'] = 'application/vnd.api+json'
    kwargs['headers']['Cookie'] = get_cookie()
    return requests.request(method, url, **kwargs)



def get_cookie():
    # id_key = __get_cookie_id_token_key()
    # id_value = os.getenv('POLLY_ID_TOKEN')
    # refresh_key = __get_cookie_refresh_token_key()
    refresh_key = "refreshToken"
    refresh_value = devpolly_token
    if refresh_key and refresh_value:
    # if id_key and id_value and refresh_key and refresh_value:
        # cookie = f'{id_key}={id_value};{refresh_key}={refresh_value}'
        cookie = f'{refresh_key}={refresh_value}'
        return cookie
    else:
        raise EnvironmentError('PollyDiscover: No valid cookies are present')


def __get_cookie_id_token_key():
    key = os.getenv('POLLY_ID_TOKEN_KEY')
    if not key:
        prefix = __get_cookie_token_key_prefix()
        key = f'{prefix}.idToken'
    return key


def __get_cookie_refresh_token_key():
    key = os.getenv('POLLY_REFRESH_TOKEN_KEY')
    if not key:
        prefix = __get_cookie_token_key_prefix()
        key = f'{prefix}.refreshToken'
    return key


def __get_cookie_token_key_prefix():
    prefix = None
    aud = os.getenv('POLLY_AUD')
    sub = os.getenv('POLLY_SUB')
    if aud and sub:
        prefix = f'CognitoIdentityServiceProvider.{aud}.{sub}'
    else:
        raise EnvironmentError("Missing env vars for cookie creation - "
                               "POLLY_AUD/POLLY_SUB")
    return prefix