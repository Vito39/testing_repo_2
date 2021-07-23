from polly.omixatlas import OmixAtlas
from json import dumps

LOG_FILE = "output.log"
open(LOG_FILE, "w").close()
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.gzTcATBCijjG7tDI-lH5euZeWxCPuamqgG6tBVfD-c5mZdWRi2s1whrhahIs5jGNYMqiE31CQvkdtZbVuJlzhVjuH353jZPiE6WJKkPUffCmMD3Kb9J0OY8iYtWEmRN3-Vql3KyTrDW0h6JDJUl20Fv86GMydXL3vY3Wh8ftqErLPD9hiDVnbvHBSlsA3TxJaIzHCIJHAIZjoMDD-H2R8vkSq9S33soJ1SNWZv6UgkXpHOr8xjxfQTCG5RQ63tVF8QGQepMZwt_mwxUzrFy4Ci_A3cY9glijuIQ0Gzc0jOoBzYwzdmIx-8uu03fW_VfayitmT-5oH_dgeEjud9VYjQ.wIe42sIoFNBrjXlI.S6J9XZshsoOoYrTkZTnnPga0LsjIv_m-aAAOYgzXlqxLiTbiQJhiI55MBC0ZYXs_-1dtSQNImmASFKuTSwbbgMqBaB7EOr8rxuifWZRVlkuH0HKw_hg_FRdufh04uHJ-7ZiXE5GWaj5iLiNvTQ42tkufQbbcRrlbPFRR7LmPaGDc_ujktg8S2kI6D2cC0iZgiD7ZPOPNUy5zemEv1k-YdoeANss4btYQGtOJAoyTeWnPN3H983w4Xjrox7rrg5ZJztTg5SWwk5oWgFBxY_OZ9EeHL01jMS9nsW4kOIPHieAWVTbWKOioAJu1UCuoemeRLv8SjXz8uPE2nW5W9NGNzQtQXChsfQyIw_PrRXWPQ8-VcQR1Cf5g-hYjF-bhauE3iN8YiMdGSBm-pw-IzzSE6PbK5_0-AH_AfqEuNjwlskbFHu3t5onJmibGmbLbbWcJCB6XKdOXhKAU6gu_tvts1boR2KioD3BDpu41Zuw6bxr2-qmE5DG0xfwKSvH7xebRatXeQ4I9uJ4itHsXlMJsxDVNCr3V_V3QmKiV2dwICEcs-jXXcG8TQrNoGeavMe4BwvzuPxTTjEUCkTzWd8j2oNKuUHnOh89j6Oc27MIVlSqusrLlO8M9MCmbs6hnLPDxk5oXbPWL5WCJ85hmg-C_5ehup59sYMdehDqOL-u8GNAwrlMPftdEM-mtq8jgg2UShB1q6pP1fOmyTZ7_iokT4yXX2RD9ljFY42HW0ra0Vz30T-dXSqGeBqJc9q27imRbORG7mu2eTkJSqa0bIspDxk_CQerb9jF0sX6LrbcrMnqQTBt5P6HB74256SA5Hl2mYCggB89vhJpUJUc6I-CR8Z2i-fa67MwyQILehNfPTF0YZGd8qBG09tJ1kKotdbCcnt-Vl_Bi-smOU3gmVacG0Tt-qywZV5qcG5DLG9w0S7Jyj-YWFDARwzyHQZp3K7zqDM-tkJEG4Q7xXoUcvyY_uHEQwG6hb-ZkvBHdKB8mi1SJwMWWeGHZRUDQCesUaOl8BFlVv87sFqX7X8P_NrDqpP-2IfpNRnbv_2CCD1J0VUP5WnjMHp7C-fdENRXCk2e0nmRyeWNbft4eau5ztfS2o9hzY3y0rDU8EVq8qN9W48vGAdSr7iroYQCnOACo5nbIF8iFos3-w3CJAC7xY3BLKLKW9oOe4hxxhW6SU7nWZL0l0dw6fPHt8-nFbydksC-vzlCtMa6ZHyL9Kf4l28z6h06wrqwh8tpTQ0k9_DvMGecavgTjQ2K1ZORWu8auBb9t9jLu2rebkwFxj0RpoTIDS0OV1aFXDoOogjYOYQpRYFf0RujizZT9CMjDs4C7bxYdvUjgb3AK0fLm.odTPJZMI77N1XB80R6pr0Q"

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
