import pathlib
from setuptools import setup, find_packages

UPSTREAM_URLLIB3_FLAG = '--with-upstream-urllib3'


def get_requirements(raw=False):
    """Build the requirements list for this project"""
    requirements_list = []

    with open('requirements.txt') as reqs:
        for install in reqs:
            if install.startswith('# only telegram.ext:'):
                if raw:
                    break
                continue
            requirements_list.append(install.strip())

    return requirements_list


def get_packages_requirements(raw=False):
    """Build the package & requirements list for this project"""
    reqs = get_requirements(raw=raw)
    exclude = ['tests*']
    packs = find_packages(exclude=exclude)
    return packs, reqs


packages, requirements = get_packages_requirements()
# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

VERSION_NUMBER="0.0.8"

# This call to setup() does all the work
# circle bracket and format in the download_url parameter to resolve linting issue
# of line too long
setup(
    name="polly-python",
    version=VERSION_NUMBER,
    description="Polly SDK",
    long_description=README,
    long_description_content_type="text/markdown",
    packages=packages,
    include_package_data=True,
    setup_requires=['wheel'],
    install_requires=requirements,
    url="https://github.com/ElucidataInc/polly-python",
    download_url=("https://elucidatainc.github.io/PublicAssets/builds/polly-python/"
                  "polly_python-{a}-none-any.whl".format(a=VERSION_NUMBER))
)
