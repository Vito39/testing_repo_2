import os
import requests
import pathlib
from polly.constants import DATA_FORMATS


def check_extension(file_path):
    file_ext = pathlib.Path(file_path).suffix
    if len(file_ext) < 2:
        return False
    url = DATA_FORMATS
    f = requests.get(url)
    split_f = f.text.split("\n")
    list_f = list(split_f)
    if file_ext in list_f:
        return True
    else:
        return False


def make_path(prefix, postfix):
    return os.path.normpath(f"{prefix}/{postfix}")
