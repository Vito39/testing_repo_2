from typing import Dict
from polly.auth import Polly
from polly.schema_ux import SchemaVisualization
import os
# key = "REFRESH_TOKEN"
# before deploying comment out these
key = "TEST_POLLY_REFRESH_TOKEN"
token = os.getenv(key)

#local development, remove after local development
# token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.ZR6wEF2daEzIO-3Qmbrx18TVdpuFCs_ttT_p19WKgyAskAlV7PvohMStFYjABJNVy-ysBdWw1YFrGS_BU_B1GBPOBWclpH0_tHd45wiV-WrE3PHE8HMtrlIfetyzMl06dRlhuB-LoWDUGe_0BbKbMEQX6nFT49oxZKtjIIMLydA-ce91rwDRwZ0XxeWmwhB00_9hFtUIk04ivABK78UysAKYzKqqLHdOzQ7ULy8jjHEOqt-5CL8LY_WHwOv5OIHm_pR9mDt4dHd7Wt9_YUO1SQf-mGZ1rODkDrRObDR2GJSXiCtWGjILZENvOAgBiEmt_WfJAw0htDeumeyTTJTSvA.6UlfYfTN0Rixdhwm.4_Kt9M0_ZZdHuta5HxmbC_tM0m3hPgm7JDiKQO4H3-fgW8aEZdtyCTnl0WS-GKsu3dhobhPuV25BjIqZccxIZaJBHuiDv_j-uyIUhZ202q-E8UTbEwpqomuHogXtdN37eMEmJce64Rqh_4l1Uo2d83AbiPFqKnU0QsB_CPQx67ipoOuJ0tbecdAqTGacPUkj4XXuIZ6NqeMyXSLGA9_9KCZ6tpf8_0BhvuYvY0UayVVoCGsZz-dQmqPPNEqlt30AaaT335yJDgd_BUvi3AzwaA9Zs7jpoMpzynm2FXdt3NrNG9IqbqqQhzrXG5UIpcZdY028eaTsu1fTLkqO4HPUO-Qq_OJ8TmxU9FE9n2-BldifDn_cALZbduE8caxdULTanKUDZaT5HslK8IjjBJMXECJ_5ff_oxg2oJA5PlwAjZFGY2OBHkTCrf7-KoDxKZZ0QXuUtvq7rHAbXDqOVoWYr0-tLw88k6n1OXElkuhyOjGWHMYR203-GpR6cR4XuTahWc4mzQS9xEHibR75-eveq0zyJnx3am9H51gQMz35JyjEUxkYAwfMBc0U2UOb57L0f9aLbtP0e6b_EveIDx0nn8lwtLQ8kN4nPZ7zfesLFSsZwiVSEEGvaQNa_ToGNSeytd8p9O6foHjR2vijA4lD7bbpMrFEUueMXIJY1dGpnK3dJHz_15ND_R0hnHF7qJVrT5bzMJYbuDArPYuWzbNom_18-quB1bKe6zK5GI8tzSXgik8w-TUX8eBvm9Wd-PaalMAUyiEffpEUe12Yx_GpMj0lP8ZC35X-qlRTIZAciaJgl5pJ9iYZbuSn1lh1bR5GUU4CCgMJn1qSzlaEAeF11n3RPIrgofjLV0aS7SHLQf1oUqcMvs3n9zH8qsatzmqDTAQpBcSHmQz3bH0tBMOPBlgI6xxBItUVzeGn8SUZbt98eGAA1kdnvQOAQ754ZkHuVaOS3wFj_Hi6WMYQYIJF2PR5y5CKU8zmjoTIN_cs9Ck6aBU9T_peOY_iC45w7_bHixx-PvabRn2nZ33lL_UwDMuuPJd45OScqHZam-JiIVq1bgg0mc0fBb7p_Tbsg_Or_yowZdl0_b9nUctG-Uo2R6h2Aj34lqVv6S_YCcBTAvexRTwcmUgwSNrqbvzf9Fn7NdZhg87wojdrPE7hGOTzWLfRzx9hNrYh0ptwmpv-_PAbaxQ14TMmf0dY3dh8-_biRT4zLgxywH68idn8L4O-BcATGPEiG40Kw5tiQsOkr969U0jbByu58JPJ7tuiQB9c78bJFpvKUOhF-WzisWYE0ug7xKFhZm-WqezSUGS0AbBgETR4S7y6rYfJ9DICaX8lnFnccrRM-Wv8.sgDPeW4EeXT0MhkvEpJsUw"

def test_obj_initialization():
    Polly.auth(token)
    assert Polly.get_session(token) is not None
    assert SchemaVisualization() is not None
    assert SchemaVisualization(token) is not None


def test_get_schema_with_repo_id_and_both_dataset_and_sample_schema_param():
    Polly.auth(token)
    repo_id = '1622526550765'
    schema_type_dict = {'dataset': 'files', 'sample': 'gct_metadata'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['dataset'] is not None
    assert schema['sample'] is not None


def test_get_schema_with_repo_id_and_sample_schema_param():
    Polly.auth(token)
    repo_id = '1622526550765'
    schema_type_dict = {'sample': 'gct_metadata'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['sample'] is not None


def test_get_schema_with_repo_id_and_dataset_schema_param():
    Polly.auth(token)
    repo_id = '1622526550765'
    schema_type_dict = {'dataset': 'files'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['dataset'] is not None


# def test_get_schema_with_empty_repo_id_and_dataset_schema_param():
#     Polly.auth(token)
#     repo_id = ''
#     schema_type_dict = {'dataset': 'files'}
#     schema_obj = SchemaVisualization(token)
#     expected_error_dict = {
#             "title": "Param Error",
#             "detail": "repo_id and schema_type_dict are either empty or its datatype is not correct"
#         }
#     error = schema_obj.get_schema(repo_id, schema_type_dict)

#     assert expected_error_dict == error


# def test_get_schema_with_repo_id_and_dataset_schema_as_list_param():
#     Polly.auth(token)
#     repo_id = ''
#     schema_type_dict = ['dataset']
#     schema_obj = SchemaVisualization(token)
#     expected_error_dict = {
#             "title": "Param Error",
#             "detail": "repo_id and schema_type_dict are either empty or its datatype is not correct"
#         }
#     error = schema_obj.get_schema(repo_id, schema_type_dict)

#     assert expected_error_dict == error


def test_get_schema_type_dataset_schema_level_single_cell_bool_false_as_params():
    Polly.auth(token)
    schema_level = ['dataset', 'sample']
    single_cell = False
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'gct_metadata'


def test_get_schema_type_dataset_schema_level_single_cell_bool_true_as_params():
    Polly.auth(token)
    schema_level = ['dataset', 'sample']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'h5ad_metadata'


def test_get_schema_type_dataset_as_params():
    Polly.auth(token)
    schema_level = ['dataset']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None


def test_get_schema_type_schema_level_single_cell_bool_true_as_params():
    Polly.auth(token)
    schema_level = ['sample']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'h5ad_metadata'


def test_get_schema_type_schema_level_single_cell_bool_false_as_params():
    Polly.auth(token)
    schema_level = ['sample']
    single_cell = False
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'gct_metadata'


# def test_get_schema_type_schema_level_empty():
#     Polly.auth(token)
#     schema_level = []  # empty
#     single_cell = False
#     schema_obj = SchemaVisualization(token)
#     error = schema_obj.get_schema_type(schema_level, single_cell)
#     print(error)
#     expected_error_dict = {
#         "title": "Param Error",
#         "detail": "schema_level is either empty or its datatype is not correct"
#     }

#     assert expected_error_dict == error


# def test_get_schema_type_schema_level_of_string_datatype():
#     Polly.auth(token)
#     schema_level = 'dataset'  # string
#     single_cell = False
#     schema_obj = SchemaVisualization(token)
#     error = schema_obj.get_schema_type(schema_level, single_cell)
#     print(error)
#     expected_error_dict = {
#         "title": "Param Error",
#         "detail": "schema_level is either empty or its datatype is not correct"
#     }

#     assert expected_error_dict == error


# def test_get_schema_type_wrong_value_of_schema_level_param():
#     Polly.auth(token)
#     schema_level = ['data']  # string
#     single_cell = False
#     schema_obj = SchemaVisualization(token)
#     error = schema_obj.get_schema_type(schema_level, single_cell)
#     print(error)
#     expected_error_dict = {
#         "title": "Incorrect Param Error",
#         "detail": "Incorrect value of param passed schema_level"
#     }

#     assert expected_error_dict == error
