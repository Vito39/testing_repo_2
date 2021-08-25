from polly.omixatlas import OmixAtlas
from json import dumps

LOG_FILE = "output.log"
open(LOG_FILE, "w").close()
REFRESH_TOKEN = "refresh_token"

repo_client = OmixAtlas(REFRESH_TOKEN)


def get_omix_details(identifier):
    response = repo_client.get_omixatlas_details(identifier)
    attributes = response["data"]["attributes"]
    try:
        indexes = attributes["indexes"]
    except KeyError:
        raise KeyError
    omix_details = {
        "repo_name": attributes["repo_name"],
        "repo_id": attributes["repo_id"],
        "indexes": indexes,
    }

    return omix_details


def get_dataset_details(dataset_index, headers, limit=20):
    query = {
        "size": 0,
        "query": {"term": {"_index": dataset_index}},
        "aggs": {
            "diseases": {"terms": {"field": "disease.keyword", "size": limit}},
            "disease-count": {"cardinality": {"field": "disease.keyword"}},
            "organisms": {
                "terms": {"field": "organism.keyword", "size": limit}
            },
            "organism-count": {"cardinality": {"field": "organism.keyword"}},
            "sources": {"terms": {"field": "dataset_source.keyword"}},
            "datatypes": {"terms": {"field": "kw_data_type.keyword"}},
        },
    }
    response_data = repo_client.search_metadata(query)

    aggregations = response_data.get("aggregations")
    data = {}

    data["dataset_count"] = response_data.get("hits").get("total").get("value")

    data["disease_count"] = aggregations.get("disease-count").get("value")
    disease_buckets = aggregations.get("diseases").get("buckets")
    data["diseases"] = [bucket.get("key") for bucket in disease_buckets]

    data["organism_count"] = aggregations.get("organism-count").get("value")
    organism_buckets = aggregations.get("organisms").get("buckets")
    data["organisms"] = [bucket.get("key") for bucket in organism_buckets]

    sources = aggregations.get("sources").get("buckets")
    data["sources"] = [source.get("key") for source in sources]

    datatypes = aggregations.get("datatypes").get("buckets")
    data["datatypes"] = [datatype.get("key") for datatype in datatypes]

    return data


def get_sample_details(sample_index, headers):
    query = {
        "size": 0,
        "query": {"term": {"_index": sample_index}},
        "track_total_hits": "true",
    }
    response_data = repo_client.search_metadata(query)

    data = {
        "sample_count": response_data.get("hits").get("total").get("value")
    }
    return data


def get_summary(identifier, headers):
    omix_details = get_omix_details(identifier)
    indexes = omix_details["indexes"]

    try:
        dataset_index = indexes["files"]
    except KeyError:
        raise Exception
    dataset_details = get_dataset_details(dataset_index, headers, limit=20)

    try:
        sample_index = indexes["gct_metadata"]
    except KeyError:
        raise Exception
    sample_details = get_sample_details(sample_index, headers)

    summary = {**omix_details, **dataset_details, **sample_details}
    response = {
        "status": None,
        "errors": None,
        "primary_data": None,
        "included_data": None,
        "meta": None,
    }
    response["status"] = 200
    response["primary_data"] = summary

    omix_details = get_omix_details(identifier)
    indexes = omix_details["indexes"]

    return response


response = get_summary("elucidata.liver_atlas", None)

print(dumps(response["primary_data"], indent=4))
