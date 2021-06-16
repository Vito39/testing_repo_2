from polly.workspaces import Workspaces

REFRESH_TOKEN = 'eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.JyuHPGvz_li1zrq8b4gL6NcyvUiU4KLS9oJgLgk4d-CZGJFLvBXyIXqhyf5jP3qSkZb002TM9bhXNwhVjTjrPEjtctf30Xnjf7RidSF9hPy4GowAL_t4IJDBMqzQyYdyqE6YOWZ1tBHa_v422iYdeXuFnD4sDOYTAUqGB_6VnxbrMuDJSG5vsNldg95UNnX1nefO6b5b9eUcvTyRfE2KbKVmRWcHv07JygxXeICq-mGx0hT5NYz_JRTKo68pZodwpKSlx4NbdI5PpSOrtjWCvTVLBqxnSIuqYQP9MsHoae3v7JAxrbom5MMkKLHFWpyjC2RlCkgiAwpOUX7LRIfpig.n9ws8UTPY16nu8PA.S9EHy6Z036H9vPudfmJ8vHSbGozUckyEhkP3krC7SQtvuNtd5Tzy-nMR2HcUvNQAGRcBRXJrfmBgKU_NBxioc3u6DRxjQ_22w0A3ESEDHOe2iHUagEMf1RGmp68hXQmz_PrI6UAvSff_7bpjkTmBWZpIDb1_GWZH6B8Ob8c9rhiRZbXK5vx4Dp-EOFDT_xRUAxCtF3NC0zhvanDvAfyrsdzMpPtkCWcglWw_cyIPKQ-vCB7dtz67P1ZGZ4IdC0BNSKCLIKkJbMTAnObe5nsESL2cUISwcWBiZkdj-NgkKpf27CDVy88VxTBWvscHO8fUE7YtftTeju8QnUyCur7chMlMU_Be_kqfNOGlMBYkqo8P2TJAttunnHRgvasLCMdj489ppW_Nm8piXpkjHqO90xTOzn80XlA1k2enaKpklJYOUs-wcxsuyXXPiDuL2KHqzIdkFUfwh1khM7prj5JtEbwmGkw68iJgJtLGTWFTVV3h1dfmtzG7jced1K9flbsNqsrUdmce6wKnedMUqWLPdhX46KNcq6tFHsAiuRjSlTXmkcjJak3Tf603cyCR8OCKHwUWF-nERNn9DsK2GHvje0K3cBH5i6k1ZgqAqU2rIaSYZagcG5zWR7vEKuApmdR8z-pyfY0d4LM_sdPorlCYdQS074_y4hYX7VYdyfLzTn7opi3SWLCuFZiJjic9dhL0NCi5l94rMm2UygPag21z7pF7MZfq41HEJSsiVDHJlSOYWN0XwGU6gZOQg8Wlo3UXLfN4W9ZGeC9yYII9BWBpQktmFV6lj-r1czgxiIPGJ8x_Jr0sR4omdb3ZMi5DwpgzmMNc-V5RKp2KHiVA7tcVCb3i3AKuMItK7asbohgPKCgMtFeF8feiGOyl506AzSBOPK_FyNpauCYhP_LOLuhsXHq9_sv6duto5p7WV2MYs5rCIKdaDCiu4h5jaQznJjpMRCPTE0LuFC7hS4zNsQNKt0C_BZkebv2V6obSzLOQ3hw6FsRJW-FGybNGnKZig4q3TnLXuDKUi80BztIOf3EFr5swCk9y0VCDtiiUWVNLwVSvoBupPLYVJKL1_rF9b_bNb4RAGTMJ31yo0K8AxG0eAMzghqajGynDfzgFOHYGo-QAugnnNTlhbbZAFd10LWpJbf1iEKjjlhImL_YFdSCNfmJsABn2ihobNAI4lCfU4UTgQ9gmOq77GY2C7oddAvKaJPzjPPi4ksW4Os2NPeO7lSfkkIcMrEzyjokZzZh3f7svOeLGrLza4Rdln37XsWGeu-a6FbqZ0EeiplNshrz_U9Qp4v5fwJoPyoeIz814SqTiKavVjBve1lRL2DU.cQiJ-215_ZS_fUMj7Wp-Sw'

workspace_client = Workspaces(REFRESH_TOKEN)

# List of Workspaces
list_of_workspaces = workspace_client.list_all_workspaces()
print(list_of_workspaces)

created_workspace_data = workspace_client.create_workspace(
    name="Workspace 11:44", description="Created from Postpolly")
workspace_id = created_workspace_data['data']['id']
workspace_client.set_workspace_id(workspace_id)

recently_created_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_created_data)

workspace_client.update_workspace(
    workspace_id=workspace_id,
    name="Workspace 11:50",
    description="Updated from Postpolly",
)
recently_updated_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_updated_data)

workspace_client.delete_workspace(workspace_id)
recently_deleted_data = workspace_client.get_workspace_by_id(workspace_id)
print(recently_deleted_data)
