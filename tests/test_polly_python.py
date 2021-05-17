#!/usr/bin/env python

"""Tests for `polly_python` package."""

import pytest


import polly_python
# from polly_python import polly_python
# from polly_python.models.workspace.workspace import WorkspaceList
# polly_python.init()
# WorkspaceListObject = WorkspaceList()
# print(WorkspaceListObject.get_workspaces())

@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string
