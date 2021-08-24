from polly.omixatlas import OmixAtlas
from json import dumps

LOG_FILE = "output.log"
open(LOG_FILE, "w").close()
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.DoI6R0eRPicaJySArxDLXYWbpwk_YAuYo_YIBaAS950oojWCyCxHd3d2_KrHgBRDBq-bNOgQQBvJUCMsMcfnOdE2zf2WTa8gKPAC-6FHYE4Mnz2UaMdWYUWh_MvU55s2W5ldNS5tteETOEsGGjRrYyriLx3VVEvuJIO9LLROQK40CKLMJnBIniVyG7Y3Ni6-IEALnX91H6qK2INN6gRPEPL9JKi2GW6N9xrUdr1Kq6q57CuaPyC-i90ir2HBznWvxbHLgV8GVRO-ROaJu0oGaCc9zEhSwQIqbcvd2kz1MaxMBpbbqfuQP-PYVmiXVKHMBUmHdVig8XNF1Ae4pLpO4g.dWlYRU39CEjQksbk.1e9Zs8MWhM4cVxNYNj6VcB5Z-CWeZp4ns622qpUjwFConM95034m6YJzEda2qRyECHm4SwAgscbX-LWUs5a6z_NOrASqIZgWvBjIm_KGJtn7dAwCo1JXHxeITwJTtvabDTtFq1DsSKt6JaY7CzaXDkTJoI9XTUrttzdRMqbKi2_zmqpxfBn25WLqcTVm_AHi3DI6pcUJXeJbBuYe7-lF2Bgky5Qrm5l2S98DVePzu1UWRaooy5Zk_g3g_cJgZ6h3fG9DRMiE4H0Qt6A-kCz_Xyy3VjmV6NdVXsytrXg4BwsAsMp1Du2AZug6feSsbpyqfAmfTOMjpttNvqpjWp6lZsdq_6QEaQpTA0aPRxJb5CV4o2bd9HDUP77Knc1McZWQrLtnNK7zuuZMTmoxbVlPcG0kvvBogRQXuwVYy4LSL6ddDjvntOjQgnDBO-_Q2G4vr63XWQoRJPc7h9b6TCIUG-zbPN7uuOENxg2D2Cu1KPvwdfbqNlhMuReENk228br9HagaE3ujcG8oGLQyMLmJUtMSgaUGbRHqaDj21IxrxUcvt8-mgV0lbiSigCx6CMN0CBgCXrts3KC7VZ5mkGDuxKx5xitNoBov1aBmht9VSX9o8RjSj7zb0KJxUHWFphF9tAxr5_8m-vmXxH5rEKjEtuSOZD4VaQiJw__3tVV37qaLZO8uW2kHm9ezreSckCyXHGB49cdaEVWXC8XWmMu-NX_4mzTRYHfVe6wtwmWmQhZWN2yhA6gTm8NpH0Es4ARSrl6fHyGXUHBbUZ9DgD0l-86LOS5UUHAdf0oViGIEqPppWhp82r3YKD_6Zas1RL5AoZYSPpNmAqf9o_MRxsaPCaqzcy_DpLe3q1k4bfepsOQQwEt37h_lhdyyVA34xJtckkdFksnwUgW7dXgjuu-mLX17A7jpuTppSL_O_-ri-WL68EASQwbXjBfs0wwAsz5ME_114fxqsMaWzvWVwcCImeiQ3PPldwn0wIoug-neBC2Q7vY5f-oAiEOlkerNn3zG4An-E6-9dyBghTIpjo5PgCb6IStSliT7XaR-gPSNb4WpNzPeDCNdRaeqh2FQolKIfSaxpeOOSw34I4rqUeyCUPZ18YkXM-YXdyM0Q7VqX6lUPnXT3o32CMCp8hYS99oYfqLqz2bw9vBqVgGm1kZH7SPB-4uzQ_cuUgemZ85MglIh0qCNdnlbFS7iq8sxhZCMcWX3aGfF3J1Oku_6dl-BxWZ5YrA_O3H4TLrN3j-9bRpnRHNqidERDNjBC0IrmPSpWejGDrhIaLu3cNRThWn_fCN2TsqO52aKER7bGd9Bsumc4MrRUWAgxxrqP0E.jx5chvPTME7hKuAJINnMfg"
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
from polly.omixatlas import OmixAtlas
library_client = OmixAtlas(REFRESH_TOKEN)

user_query = "SELECT * from liveromix_atlas.datasets WHERE disease='nash' LIMIT 100"
result_wo_expansion = library_client.query_metadata(query=user_query)
result_with_expansion = library_client.query_metadata(query=user_query, experimental_features = {"expand":True, "related_terms":False})
result_with_max_expansion = library_client.query_metadata(query=user_query, experimental_features = {"expand":True, "related_terms":True})

# expanded_query = "SELECT * from liveromix_atlas_files WHERE disease in ('nash - nonalcoholic steatohepatitis', 'non-alcoholic fatty liver disease', 'non alcoholic steatohepatitis', 'nash', 'non-alcoholic steatohepatitis', 'nonalcoholic steatohepatitis', 'inflammatory disease') AND organism = 'Homo sapiens' LIMIT 100"

