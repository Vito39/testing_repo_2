from polly import workspaces
import os
key = "REFRESH_TOKEN"
token = os.getenv(key)


def test_obj_initialised():
    assert workspaces.Workspaces(token) is not None


def test_fetch_my_workspaces():
    obj = workspaces.Workspaces(token)
    assert dict(obj.fetch_my_workspaces()) is not None
