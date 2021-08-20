from polly import omixatlas
import pytest
import os
key = "REFRESH_TOKEN"
token = os.getenv(key)



def test_obj_initialised():
    assert omixatlas.OmixAtlas(token) != None


def test_get_all_omixatlas():
    obj = omixatlas.OmixAtlas(token)
    assert obj.get_all_omixatlas()['data'] != None


def test_omixatlas_summary():
    obj1 = omixatlas.OmixAtlas(token)
    key = 'elucidata.liveromix_atlas'
    assert obj1.omixatlas_summary(key)['data'] != None


def test_download_data():
    obj2 = omixatlas.OmixAtlas(token)
    repo_name = 'elucidata.liveromix_atlas'
    d_id = 'CCLE_metabolomics_LIVER'
    assert obj2.download_data('elucidata.liveromix_atlas','CCLE_metabolomics_LIVER')['data'] != None


def test_query_metadata():
    query_dataset_level = "select  description, kw_data_type, organism, disease, tissue, kw_drug, kw_cell_line, kw_cell_type, dataset_source, publication, total_num_samples, total_num_cells FROM liveromix_atlas_files WHERE disease = 'normal' AND organism = 'Homo sapiens' AND kw_drug = 'none' AND dataset_source = 'GEO' AND tissue IN('liver','bone','none') AND kw_data_type = 'transcriptomics' AND kw_cell_line IN ('none', 'HEP-G2') AND kw_cell_type IN ('none',  'hepatocyte', 'embryonic fibroblast') AND publication = Match_Query('https://www.ncbi.nlm.nih.gov/geo/query/') AND description = Match_Phrase('gene') AND total_num_samples > 4"
    query_sample_level = "select sample_id, kw_curated_disease, kw_curated_cell_line, kw_curated_drug, kw_curated_cell_type, kw_curated_genetic_mod_type, kw_curated_modified_gene FROM liveromix_atlas_gct_metadata where kw_curated_cell_type = 'hepatocyte' AND kw_curated_disease = 'normal' AND kw_curated_cell_line = 'none' AND kw_curated_drug = 'none' AND kw_curated_genetic_mod_type = 'none' AND kw_curated_modified_gene = 'none'"
    query_feature_level = "select kw_doc_id, disease FROM liveromix_atlas_gct_data WHERE kw_doc_id = 'discover-test-datalake-v1@@@liver_atlas_test@@data@@Methylation@@LIHC_Methylation_TCGA.gct'"
    query_singlecell_sample = "select  sample, platform, title, organism_ch1, source_name_ch1, batch, umi_counts, umi_counts_log, gene_counts, gene_counts_log, percent_mito, clusters, kw_column, kw_timestamp,  kw_doc_id FROM liveromix_atlas_h5ad_metadata WHERE sample = Match_Query('GSM2877959')"
    query_singlecell_feature = "select  * FROM liveromix_atlas_h5ad_data"
    obj3 = omixatlas.OmixAtlas(token)
    assert obj3.query_metadata(query_feature_level) != None
    assert dict(obj3.query_metadata(query_sample_level)) != None
    assert dict(obj3.query_metadata(query_dataset_level)) != None
    assert dict(obj3.query_metadata(query_singlecell_sample)) != None
    assert dict(obj3.query_metadata(query_singlecell_feature)) != None
