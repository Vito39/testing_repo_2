from polly import omixatlas
import os
import csv
key = "REFRESH_TOKEN"
token = os.getenv(key)


def test_obj_initialised():
    assert omixatlas.OmixAtlas(token) is not None


def test_get_all_omixatlas():
    obj = omixatlas.OmixAtlas(token)
    assert obj.get_all_omixatlas()['data'] is not None


def test_omixatlas_summary():
    obj1 = omixatlas.OmixAtlas(token)
    key = 'elucidata.liveromix_atlas'
    assert obj1.omixatlas_summary(key)['data'] is not None


def test_download_data():
    obj2 = omixatlas.OmixAtlas(token)
    repo_name = 'elucidata.liveromix_atlas'
    d_id = 'CCLE_metabolomics_LIVER'
    assert obj2.download_data(repo_name, d_id)['data'] is not None


def test_query_metadata():
    query_dict = {}
    with open('tests/query.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                query_dict[row[0]] = row[1]
    query_dataset_level = query_dict['query_dataset_level']
    query_sample_level = query_dict['query_sample_level']
    query_feature_level = query_dict['query_feature_level']
    query_singlecell_sample = query_dict['query_singlecell_sample']
    query_singlecell_feature = query_dict['query_singlecell_feature']
    obj3 = omixatlas.OmixAtlas(token)
    assert dict(
        obj3.query_metadata(query_feature_level, query_api_version="v1")
    ) is not None
    assert dict(
        obj3.query_metadata(query_sample_level, query_api_version="v1")
    ) is not None
    assert dict(
        obj3.query_metadata(query_dataset_level, query_api_version="v1")
    ) is not None
    assert dict(
        obj3.query_metadata(query_singlecell_sample, query_api_version="v1")
    ) is not None
    assert dict(
        obj3.query_metadata(query_singlecell_feature, query_api_version="v1")
    ) is not None
