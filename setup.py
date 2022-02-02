import pathlib
import pkg_resources
try:
    pkg_resources.require(['pip >= 18.1'])
except Exception:
    raise RuntimeError("Error: Need pip > 18.1 upgrade pip to the latest version with 'pip install --upgrade pip'") from None
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

# This call to setup() does all the work
setup(
    name="polly-python",
    version="0.0.6",
    description="Polly SDK",
    long_description=README,
    long_description_content_type="text/markdown",
    packages=packages,
    include_package_data=True,
    setup_requires=['wheel'],
    install_requires=requirements,
    url="https://github.com/ElucidataInc/polly-python",
    download_url=("https://elucidatainc.github.io/PublicAssets/builds/"
                  "polly-python/tests/polly/polly_python-0.0.6-py3-none-any.whl"),
)
