from polly.omixatlas import OmixAtlas

LOG_FILE = "output.log"
open(LOG_FILE, "w").close()
REFRESH_TOKEN = "refresh_token"
repo_client = OmixAtlas(REFRESH_TOKEN)

# Get All Omixatlases

# all_omixatlases = repo_client.get_all_omixatlas()

# with open(LOG_FILE, "a") as logfile:
#     logfile.write("\n\nAll Omixatlases")
#     logfile.write(dumps(all_omixatlases, indent=4))

# Get Omixatlas by Name

repo_name = "elucidata.liveromix_atlas"

# repo_details = repo_client.omixatlas_summary(repo_name)

# with open(LOG_FILE, "a") as logfile:
#     logfile.write("\n\n Omixatlas Details with Name\n")
#     logfile.write(dumps(repo_details, indent=4))

# Get Omixatlas by ID

repo_id = "1615965444377"

# repo_details = repo_client.omixatlas_summary(repo_name)

# with open(LOG_FILE, "a") as logfile:
#     logfile.write("\n\n Omixatlas Details with ID\n")
#     logfile.write(dumps(repo_details, indent=4))

# SQL Queries

query = "SELECT file_type FROM liveromix_atlas_files LIMIT 100,2000"  # datasets

query = "SELECT * FROM liveromix_atlas_gct_metadata"  # samples

query = "SELECT * FROM liveromix_atlas_gct_data"  # features

query = "SELECT * FROM liveromix_atlas_h5ad_data"  # features single cell

query = "SELECT * FROM liveromix_atlas_h5ad_metadata"  # features single cell

query = "SELECT file_Type FROM liveromix_atlas_files"  # semantic analysis exception

query = "SELECT file_type FROM liveromix_atas_files"  # index not found exception

query = "SELECT file_type FROM liveromix_atlas_files LIMIT 20000"  # invalid limit

# forbidden, only select allowed
query = "DELETE file_type FROM liveromix_atlas_files"

# query_response = repo_client.query_metadata(query)

# print(query_response)

# with open(LOG_FILE, "a") as logfile:
#     logfile.write("\n\n Metadata Query\n")
#     try:
#         logfile.write(query_response.to_string())
#     except AttributeError:
#         logfilete(dumps(query_response, indent=4))


# Download Data

query = "SELECT dataset_id FROM liveromix_atlas_files"
query_response = repo_client.query_metadata(query)

print(query_response)

# with open(LOG_FILE, "a") as logfile:
#     logfile.write("\n\n Metadata Query\n")
#     logfile.write(dumps(search_response, indent=4))

# repo_name = 'elucidata.liver_atlas'

data_id = query_response.iloc[0]

response = repo_client.download_data(repo_name, data_id)
signed_url = response.get("data")
print(signed_url)


# expansion use cases-

library_client = OmixAtlas(REFRESH_TOKEN)

user_query = "SELECT * from liveromix_atlas.datasets WHERE disease='nash' LIMIT 100"
result_wo_expansion = library_client.query_metadata(query=user_query)
result_with_expansion = library_client.query_metadata(
    query=user_query, experimental_features={"expand": True, "related_terms": False}
)
result_with_max_expansion = library_client.query_metadata(
    query=user_query, experimental_features={"expand": True, "related_terms": True}
)
# expanded_query = "SELECT * from liveromix_atlas_files WHERE disease in ('nash - nonalcoholic steatohepatitis',
# 'non-alcoholic fatty liver disease', 'non alcoholic steatohepatitis', 'nash', 'non-alcoholic steatohepatitis',
# 'nonalcoholic steatohepatitis', 'inflammatory disease') AND organism = 'Homo sapiens' LIMIT 100"
