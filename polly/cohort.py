import os
import ssl
import json
import base64
from polly import helpers
from pathlib import Path
import urllib.request
import logging
import pandas as pd
from polly.auth import Polly
from polly.omixatlas import OmixAtlas
from polly.errors import (
    InvalidCohortNameException,
    InvalidCohortOperationException,
    paramException,
    InvalidParameterException,
    MissingKeyException,
    OperationFailedException,
    InvalidPathException,
    InvalidRepoException,
    InvalidDatasetException,
    EmptyCohortException,
    InvalidCohortPathException,
    error_handler,
)
from polly.constants import COHORT_REPO_INFO, VALID_REPO_NAMES, dot
import shutil
from joblib import Parallel, delayed
import datetime
from cmapPy.pandasGEXpress.parse import parse
from cmapPy.pandasGEXpress.concat import assemble_common_meta, assemble_data


class Cohort:
    def __init__(self, token=None, env="polly") -> None:
        self.session = Polly.get_session(token, env=env)
        self.base_url = f"https://v2.api.{self.session.env}.elucidata.io"
        self.folder_path = None
        self._cohort_details = None

    def _read_gcts(self, dataset_ids: list, file_path: str) -> pd.DataFrame:
        gct_files = [
            f"{file_path}/{self._cohort_details.get('entity_id',{}).get(dataset_id)[0]}_{dataset_id}.gct"
            for dataset_id in dataset_ids
        ]
        results_gct = Parallel(n_jobs=4)(
            delayed(parse)(gct_file) for gct_file in gct_files
        )
        return results_gct

    def merge_metadata(self):
        """
        Function to merge the gct files in a cohort and return merged meta data
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        file_path = self.folder_path
        sample_list = list(self._cohort_details["entity_id"].keys())
        if len(sample_list) == 0:
            raise EmptyCohortException
        results_gct = self._read_gcts(sample_list, file_path)
        gct_files = [dataset_id + ".gct" for dataset_id in sample_list]
        All_Metadata = assemble_common_meta(
            [i.col_metadata_df for i in results_gct],
            fields_to_remove=[],
            sources=gct_files,
            remove_all_metadata_fields=False,
            error_report_file="errors",
        )
        return All_Metadata

    def merge_data_matrix(self) -> pd.DataFrame:
        """
        Function to merge the gct files in a cohort and return merged data matrix
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        file_path = self.folder_path
        sample_list = list(self._cohort_details["entity_id"].keys())
        if len(sample_list) == 0:
            raise EmptyCohortException
        results_gct = self._read_gcts(sample_list, file_path)
        All_data_matrix = assemble_data(
            [i.data_df for i in results_gct], concat_direction="horiz"
        )
        return All_data_matrix

    def edit_cohort(self, new_cohort_name: str, new_desc: str):
        """
        Function to edit a cohort name and description
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        if dot in new_cohort_name:
            raise InvalidCohortNameException(new_cohort_name)
        if not (new_desc and isinstance(new_desc, str)):
            raise InvalidParameterException("description")
        p = Path(self.folder_path)
        parent = p.parent
        str_parent = str(parent.resolve())
        new_path = helpers.make_path(str_parent, f"{new_cohort_name}.pco")
        existing_path = self.folder_path
        meta_path = f"{existing_path}/.meta"
        with open(meta_path, "r+b") as openfile:
            byte = openfile.read()
            data = base64.b64decode((byte))
            json_data = json.loads(data.decode("utf-8"))
            os.rename(existing_path, new_path)
            json_data["description"] = new_desc
            self._cohort_details = json_data
            self.folder_path = new_path
            input = json.dumps(json_data)
            encoded_data = base64.b64encode(input.encode("utf-8"))
            openfile.seek(0)
            openfile.write(encoded_data)
            openfile.truncate()
        logging.basicConfig(level=logging.INFO)
        logging.info("Cohort Edited!")

    def is_valid(self) -> bool:
        """
        Function to check if a cohort is valid or not
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        if not os.path.exists(self.folder_path):
            raise InvalidPathException
        meta_path = f"{self.folder_path}/.meta"
        if not os.path.exists(meta_path):
            return False
        sample_list = list(self._cohort_details["entity_id"].keys())
        if len(sample_list) == 0:
            return True
        for sample in sample_list:
            omixatlas = self._cohort_details.get("entity_id", {}).get(sample)[0]
            gct_path = f"{self.folder_path}/{omixatlas}_{sample}.gct"
            jpco_path = f"{self.folder_path}/{omixatlas}_{sample}.jpco"
            if not (os.path.exists(gct_path) and os.path.exists(jpco_path)):
                return False
        return True

    def delete_cohort(self) -> None:
        """
        Function to delete a cohort
        """
        shutil.rmtree(self.folder_path, ignore_errors=True)
        logging.basicConfig(level=logging.INFO)
        logging.info("Cohort Deleted Successfuly!")
        self.folder_path = None
        self._cohort_details = None

    def remove_from_cohort(self, dataset_ids: list) -> None:
        """
        Function for removing dataset/s from a cohort
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        if not (dataset_ids and isinstance(dataset_ids, list)):
            raise InvalidParameterException("entity_id")
        dataset_count = 0
        verified_dataset = []
        file_meta = helpers.make_path(self.folder_path, ".meta")
        with open(file_meta, "r+b") as openfile:
            byte = openfile.read()
            data = base64.b64decode((byte))
            json_data = json.loads(data.decode("utf-8"))
            dataset_id = list(json_data["entity_id"].keys())
            for dataset in dataset_ids:
                if dataset not in dataset_id:
                    logging.basicConfig(level=logging.INFO)
                    logging.info(f"Dataset Id - {dataset} not present in the Cohort.")
                    continue
                dataset_count += 1
                verified_dataset.append(dataset)
                omixatlas = json_data.get("entity_id", {}).get(dataset)[0]
                gct_path = f"{self.folder_path}/{omixatlas}_{dataset}.gct"
                json_path = f"{self.folder_path}/{omixatlas}_{dataset}.jpco"
                os.remove(gct_path)
                os.remove(json_path)
                del json_data.get("entity_id")[dataset]
                json_data.get("source_omixatlas").get(omixatlas).remove(dataset)
            omixatlas_dict = json_data.get("source_omixatlas")
            empty_keys = []
            for key, value in omixatlas_dict.items():
                if value == []:
                    empty_keys.append(key)
                    # del omixatlas_dict[key]
            for key in empty_keys:
                del omixatlas_dict[key]
            json_data["number_of_samples"] -= dataset_count
            json_data["source_omixatlas"] = omixatlas_dict
            if not bool(json_data.get("entity_id")):
                del json_data["entity_type"]
            self._cohort_details = json_data
            input = json.dumps(json_data)
            encoded_data = base64.b64encode(input.encode("utf-8"))
            openfile.seek(0)
            openfile.write(encoded_data)
            openfile.truncate()
        logging.basicConfig(level=logging.INFO)
        logging.info(f"'{dataset_count}' dataset/s removed from Cohort!")

    def add_to_cohort(self, repo_key: str, dataset_id: list) -> None:
        """
        Function to add dataset/s to a cohort
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        if not (repo_key and isinstance(repo_key, str)):
            raise InvalidParameterException("repo_key")
        if not (dataset_id and isinstance(dataset_id, list)):
            raise InvalidParameterException("entity_id")
        obj = OmixAtlas()
        local_path = self.folder_path
        response_omixatlas = obj.omixatlas_summary(repo_key)
        data = response_omixatlas.get("data")
        repo_name = data.get("repo_name")
        dataset_id = self._validate_repo(repo_name, dataset_id)
        entity_type = self._get_entity(repo_name)
        Parallel(n_jobs=20, require="sharedmem")(
            delayed(self._gctfile)(repo_name, i, local_path) for i in dataset_id
        )
        Parallel(n_jobs=20, require="sharedmem")(
            delayed(self._add_metadata)(repo_name, i, local_path) for i in dataset_id
        )
        file_meta = helpers.make_path(local_path, ".meta")
        with open(file_meta, "r+b") as openfile:
            # data = json.load(openfile)
            byte = openfile.read()
            data = base64.b64decode((byte))
            json_data = json.loads(data.decode("utf-8"))
            source_omixatlas = json_data.get("source_omixatlas")
            if "entity_type" not in json_data:
                json_data["entity_type"] = entity_type
            if repo_name not in source_omixatlas:
                source_omixatlas[repo_name] = dataset_id
            else:
                [source_omixatlas[repo_name].append(i) for i in dataset_id]
            for i in dataset_id:
                metadata = self._get_metadata(repo_name, i)
                dataset_tuple = [
                    repo_name,
                    metadata.get("_source", {}).get("kw_data_type"),
                ]
                json_data["entity_id"][i] = dataset_tuple
                json_data["number_of_samples"] += 1
            self._cohort_details = json_data
            input = json.dumps(json_data)
            encoded_data = base64.b64encode(input.encode("utf-8"))
            openfile.seek(0)
            openfile.write(encoded_data)
            openfile.truncate()
        logging.basicConfig(level=logging.INFO)
        logging.info(f"'{len(dataset_id)}' dataset/s added to Cohort!")

    def _add_metadata(self, repo_key: str, dataset_id: str, local_path: str) -> None:
        """
        Function to add dataset level metadata to a cohort.
        """
        if not (repo_key and isinstance(repo_key, str)):
            raise InvalidParameterException("repo_id/repo_name")
        if not (dataset_id and isinstance(dataset_id, str)):
            raise InvalidParameterException("dataset_id")
        metadata = self._get_metadata(repo_key, dataset_id)
        file_name = f"{repo_key}_{dataset_id}.jpco"
        file_name = helpers.make_path(local_path, file_name)
        with open(file_name, "w") as outfile:
            json.dump(metadata, outfile)

    def _gctfile(self, repo_info: str, dataset_id: str, file_path: str) -> None:
        """
        Function to add gct file to a cohort
        """
        if not (repo_info and isinstance(repo_info, str)):
            raise InvalidParameterException("repo_name/repo_id")
        if not (dataset_id and isinstance(dataset_id, str)):
            raise InvalidParameterException("dataset_id")
        ssl._create_default_https_context = ssl._create_unverified_context
        obj = OmixAtlas()
        download_dict = obj.download_data(repo_info, dataset_id)
        url = download_dict.get("data", {}).get("attributes", {}).get("download_url")
        if not url:
            raise MissingKeyException("dataset url")
        file_name = f"{repo_info}_{dataset_id}.gct"
        dest_path = helpers.make_path(file_path, file_name)
        try:
            urllib.request.urlretrieve(url, dest_path)
        except Exception as e:
            raise OperationFailedException(e)

    def create_cohort(
        self,
        local_path: str,
        cohort_name: str,
        description: str,
        repo_key=None,
        dataset_id=None,
    ) -> None:
        """
        Function to create a cohort
        """
        if not (local_path and isinstance(local_path, str)):
            raise InvalidParameterException("repo_id/repo_name")
        if not (cohort_name and isinstance(cohort_name, str)):
            raise InvalidParameterException("dataset_id")
        if not (description and isinstance(description, str)):
            raise InvalidParameterException("dataset_id")
        if not os.path.exists(local_path):
            raise InvalidPathException  # make a better exception class
        if dot in cohort_name:
            raise InvalidCohortNameException(cohort_name)
        file_path = os.path.join(local_path, f"{cohort_name}.pco")
        user_id = self._get_user_id()
        os.makedirs(file_path)
        metadata = {
            "number_of_samples": 0,
            "entity_id": {},
            "source_omixatlas": {},
            "description": description,
            "user_id": user_id,
            "date_created": str(datetime.datetime.now()),
            "version": "0.1",
        }
        file_name = os.path.join(file_path, ".meta")
        input = json.dumps(metadata)
        with open(file_name, "wb") as outfile:
            encoded_data = base64.b64encode(input.encode("utf-8"))
            outfile.write(encoded_data)
        self.folder_path = str(file_path)
        self._cohort_details = metadata
        if dataset_id is not None and repo_key is not None:
            self.add_to_cohort(repo_key, dataset_id)
        logging.basicConfig(level=logging.INFO)
        logging.info("Cohort Created !")

    def _get_user_id(self):
        """
        Function to get user id
        """
        me_url = f"{self.base_url}/users/me"
        details = self.session.get(me_url)
        error_handler(details)
        user_id = details.json().get("data", {}).get("attributes", {}).get("user_id")
        return user_id

    def summarize_cohort(self):
        """
        Function to return metadata and summary of a cohort
        """
        if self._cohort_details is None:
            raise InvalidCohortOperationException
        meta_details = self._get_metadetails()
        df_details = self._get_df()
        return meta_details, df_details

    def load_cohort(self, local_path: str):
        """
        Function to load an existing cohort into an object
        """
        if not os.path.exists(local_path):
            raise InvalidPathException(local_path)
        file_meta = helpers.make_path(local_path, ".meta")
        if not os.path.exists(file_meta):
            raise InvalidCohortPathException
        file = open(file_meta, "r")
        byte = file.read()
        file.close()
        data = base64.b64decode((byte))
        str_data = data.decode("utf-8")
        json_data = json.loads(str_data)
        self._cohort_details = json_data
        self.folder_path = local_path
        logging.basicConfig(level=logging.INFO)
        logging.info("Cohort Loaded !")

    def _get_metadetails(self) -> dict:
        """
        Function to return metadata details of a cohort
        """
        data = self._cohort_details
        meta_dict = {}
        meta_details = ["description", "number_of_samples"]
        for key, value in data.items():
            if key.lower() in meta_details:
                meta_dict[key] = value
        return meta_dict

    def _get_df(self) -> pd.DataFrame:
        """
        Function to return cohort summary in a dataframe
        """
        data = self._cohort_details
        df_dict = {"source_omixatlas": [], "entity_id": [], "datatype": []}
        dataset_list = list(data["entity_id"].keys())
        for entity in dataset_list:
            omixatlas = data.get("entity_id", {}).get(entity, {})[0]
            data_type = data.get("entity_id", {}).get(entity, {})[1]
            df_dict["entity_id"].append(entity)
            df_dict["source_omixatlas"].append(omixatlas)
            df_dict["datatype"].append(data_type)
        dataframe = pd.DataFrame.from_dict(df_dict)
        return dataframe

    def _get_metadata(self, repo_key: str, dataset_id: str) -> dict:
        """
        Function to return metadata for a dataset
        """
        obj = OmixAtlas()
        response_omixatlas = obj.omixatlas_summary(repo_key)
        data = response_omixatlas.get("data")
        index_name = data.get("indexes", {}).get("files")
        if index_name is None:
            raise paramException(
                title="Param Error", detail="Repo entered is not an omixatlas."
            )
        elastic_url = f"{obj.elastic_url}/{index_name}/_search"
        query = helpers.elastic_query(index_name, dataset_id)
        metadata = helpers.get_metadata(obj, elastic_url, query)
        return metadata

    def _validate_repo(self, repo_name: str, dataset_id: list) -> list:
        """
        Function to validate repo and datasets given as argument for adding to cohort
        """
        dict_dataset = self._cohort_details.get("source_omixatlas")
        dataset_list = list(self._cohort_details["entity_id"].keys())
        if bool(dict_dataset):
            repo_list = list(dict_dataset.keys())
            if repo_name not in repo_list:
                raise InvalidRepoException(repo_name)
        if repo_name not in VALID_REPO_NAMES:
            raise InvalidRepoException(repo_name)
        valid_dataset_id = []
        for dataset in dataset_id:
            metadata = self._get_metadata(repo_name, dataset)
            data_type = metadata.get("_source", {}).get("kw_data_type")
            if data_type.lower() not in VALID_REPO_NAMES[repo_name]:
                logging.basicConfig(level=logging.INFO)
                logging.info(
                    "This feature supports 'Datatype' = 'mutation' and 'transcriptomics' ."
                )
                continue
            if dataset in dataset_list:
                logging.basicConfig(level=logging.INFO)
                logging.info(
                    f"The entity_id - {dataset} is already existing in the cohort."
                )
                continue
            valid_dataset_id.append(dataset)
        if len(valid_dataset_id) == 0:
            raise InvalidDatasetException
        return valid_dataset_id

    def _get_entity(self, repo_name: str) -> str:
        """
        Function to get the entity type of repo
        """
        for repo, dict in COHORT_REPO_INFO.items():
            if repo_name == repo:
                if dict["file_structure"] == "single":
                    entity_type = "dataset"
                else:
                    entity_type = "sample"
        return entity_type
