import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="polly-omixatlas",
    version="0.0.3",
    description="Polly SDK",
    long_description=README,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "certifi",
        "chardet",
        "idna",
        "postpy2",
        "python-magic",
        "requests",
        "urllib3",
    ],
)
