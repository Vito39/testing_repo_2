from typing import Dict
from polly.auth import Polly
from polly.schema_ux import SchemaVisualization
import os

key = "TEST_POLLY_REFRESH_TOKEN"
token = os.getenv(key)


def test_obj_initialization():
    assert Polly.get_session(token) is not None
    assert SchemaVisualization(token) is not None


def test_get_schema_with_repo_id_and_both_dataset_and_sample_schema_param():
    repo_id = '1622526550765'
    schema_type_dict = {'dataset': 'files', 'sample': 'gct_metadata'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['dataset'] is not None
    assert schema['sample'] is not None


def test_get_schema_with_repo_id_and_sample_schema_param():
    repo_id = '1622526550765'
    schema_type_dict = {'sample': 'gct_metadata'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['sample'] is not None


def test_get_schema_with_repo_id_and_dataset_schema_param():
    repo_id = '1622526550765'
    schema_type_dict = {'dataset': 'files'}
    schema_obj = SchemaVisualization(token)
    schema = schema_obj.get_schema(repo_id, schema_type_dict)
    assert isinstance(schema, Dict)
    assert schema['dataset'] is not None


def test_get_schema_type_dataset_schema_level_single_cell_bool_false_as_params():
    schema_level = ['dataset', 'sample']
    single_cell = False
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'gct_metadata'


def test_get_schema_type_dataset_schema_level_single_cell_bool_true_as_params():
    schema_level = ['dataset', 'sample']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'h5ad_metadata'


def test_get_schema_type_dataset_as_params():
    schema_level = ['dataset']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['dataset'] is not None


def test_get_schema_type_schema_level_single_cell_bool_true_as_params():
    schema_level = ['sample']
    single_cell = True
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'h5ad_metadata'


def test_get_schema_type_schema_level_single_cell_bool_false_as_params():
    schema_level = ['sample']
    single_cell = False
    schema_obj = SchemaVisualization(token)
    schema_type = schema_obj.get_schema_type(schema_level, single_cell)
    assert isinstance(schema_type, Dict)
    assert schema_type['sample'] is not None
    assert schema_type['sample'] == 'gct_metadata'
