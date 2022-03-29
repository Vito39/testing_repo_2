# from polly import constants as const
# from polly.env import DISCOVER_API_URL
# from polly.helpers import make_discover_request


# class Package(object):
#     """
#     Package Model
#     """

#     REPOSITORY_PACKAGE_URL = (
#         DISCOVER_API_URL + const.REPOSITORY_PACKAGE_ENDPOINT
#     )

#     def __init__(self, package_name, repo_id):
#         """
#         Create a new Package object

#         Args:
#             package_name (str): Package to be created
#             repo_id (str): Repository ID

#         Raises:
#             ValueError: if package_name is not a string or is empty
#             ValueError: if repo_id is not a string/int or is empty
#         """
#         if not isinstance(package_name, str) or not package_name:
#             raise ValueError("package_name must be a str and cannot be empty")

#         if (
#             not isinstance(repo_id, str) and not isinstance(repo_id, int)
#         ) or not repo_id:
#             raise ValueError("repo_id must be a str or int and cannot be empty")

#         self.package_name = package_name
#         self.repo_id = str(repo_id)

#         self.REPOSITORY_PACKAGE_URL = self.REPOSITORY_PACKAGE_URL.format(
#             repo_id
#         )

#     def create(self):
#         """
#         Create an entry in Package table

#         Raises:
#             Exception: if Package already exists in the repository
#             Exception: if Package creation fails

#         Returns:
#             package (Package): created package response
#         """
#         repository_package = self.get(self.repo_id)
#         if repository_package is not None:
#             raise Exception("package already exists for this repository")

#         payload = get_package_payload(self.package_name)
#         resp = make_discover_request(
#             "POST", self.REPOSITORY_PACKAGE_URL, json=payload
#         )
#         if resp.status_code == const.CREATED:
#             return resp.json()["data"].get("attributes")
#         else:
#             raise Exception(resp.text)

#     @classmethod
#     def get(cls, repo_id):
#         """
#         Get packages of a repository

#         Args:
#             repo_id (str): Repository ID

#         Raises:
#             Exception: if failed to get packages

#         Returns:
#             package : Package if exist else None
#         """
#         if (
#             not isinstance(repo_id, str) and not isinstance(repo_id, int)
#         ) or not repo_id:
#             raise ValueError("repo_id must be a str or int and cannot be empty")

#         resp = make_discover_request(
#             "GET", cls.REPOSITORY_PACKAGE_URL.format(str(repo_id))
#         )
#         if resp.status_code == const.OK:
#             response_data = resp.json()["data"]
#             return (
#                 response_data[0].get("attributes")
#                 if len(response_data)
#                 else None
#             )
#         else:
#             raise Exception(resp.text)


# def get_package_payload(package_name):
#     """
#     Payload for Package

#     Args:
#         package_name (str)

#     Returns:
#         dict: JSON API Request Payload
#     """
#     return {
#         "data": {
#             "type": "packages",
#             "attributes": {"package_name": package_name},
#         }
#     }

# def update(self, repo_id: int, display_name="", description=""):
    #     """
    #     """
    #     repo = self.get_omixatlas(repo_id)
    #     payload = self.get_repository_payload()
    #     frontend_info = {}
    #     print(f"--- repo info-----{repo}------")
    #     frontend_info = repo["frontend_info"]

    #     if display_name:
    #         frontend_info["display_name"] = display_name
    #         frontend_info["initials"] = self.construct_initials(display_name)
    #     if description:
    #         frontend_info["description"] = description
        
    #     del payload["data"]["attributes"]["repo_name"]
    #     del payload["data"]["attributes"]["indexes"]
    #     payload["data"]["id"] = repo_id
    #     payload["data"]["attributes"]["frontend_info"] = frontend_info
    #     payload["data"]["attributes"]["components"] = repo["components"]
    #     payload["data"]["attributes"]["studio_presets"] = repo["studio_presets"]

    #     print("-----1-------")
    #     validator.validate_repository_schema(payload["data"]["attributes"], update=True)
    #     print("-----2-------")
    #     repository_url = f"{self.discover_url}{const.REPOSITORIES_ENDPOINT}/{repo_id}"
    #     print(f"----repository-url-{repository_url}----")
    #     print(f"----json--payload--{payload}----")
    #     resp = self.session.patch(repository_url, json=payload)

    #     error_handler(resp)
    #     print("--------3-------")
    #     if resp.status_code != const.OK:
    #         raise Exception(resp.text)
    #     else:
    #         repo_id = resp.json()["data"]["id"]
    #         print(f" OmixAtlas {repo_id} Updated  ")
    #         return resp.json()

    # def get_omixatlas(self, repo_id):
    #     """
    #     """
    #     repository_url = f"{self.discover_url}/repositories/{repo_id}"
    #     response = self.session.get(repository_url)
    #     error_handler(response)
    #     return response.json()["data"]["attributes"]
    

    # def add_package(self, repo_id: int, package_name: int):
    #     """
    #     """
    #     if not isinstance(package_name, str) or not package_name:
    #             raise ValueError("package_name must be a str and cannot be empty")

    #     if (not isinstance(repo_id, str) and not isinstance(repo_id, int)) or not repo_id:
    #         raise ValueError("repo_id must be a str or int and cannot be empty")


    # def get_package(self, repo_id: int):
    #     """
    #     """
    #     if (
    #         not isinstance(repo_id, str) and not isinstance(repo_id, int)
    #     ) or not repo_id:
    #         raise ValueError("repo_id must be a str or int and cannot be empty")
        
    #     repository_package = const.REPOSITORY_PACKAGE_ENDPOINT.format(str(repo_id))
    #     print(f"-------package----{repository_package}----")
    #     repository_url = f"{self.discover_url}{repository_package}"
    #     print(f"---repository_url-----{repository_url}----")
        


    # def get_package_payload(package_name):
    #     """
    #     """
    #     return {
    #             "data": {
    #                 "type": "packages",
    #                 "attributes": {"package_name": package_name},
    #             }
    #         }    

 for omixatlas_key, omixatlas_val in all_omixatlases.items():
            print(f"----{omixatlas_val}----")
            omixatlases = omixatlas_val

all_omixatlases = self.get_all_omixatlas()
        omixatlases = all_omixatlases.get('data')
        # print("----1-----")
        # search for passed repo_name in all omixatlases
        # to see if it exists from before or not
        # for omixatlas in omixatlases:
        #     for key, value in omixatlas.items():
        #         if key == "attributes":
        #             attribute_data = value
        #             for attribute_key, attribute_value in attribute_data.items():
        #                 if attribute_key == "repo_name":
        #                     if repo_name == attribute_value:
        #                         raise ValueError(f"{repo_name} already exists in one of the omixatlases")