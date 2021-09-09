from polly import new_workspaces
import os
key = "REFRESH_TOKEN"
token = os.getenv(key)


def test_obj_initialised():
    assert new_workspaces.Workspaces(token) is not None


def test_fetch_my_workspaces():
    obj = new_workspaces.Workspaces(token)
    assert obj.fetch_my_workspaces() is True
