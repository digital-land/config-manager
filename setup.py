from setuptools import find_packages, setup

from version import get_version

setup(
    name="digital-land-data-manager",
    version=get_version(),
    author="DLUHC Digital Land Team",
    author_email="DigitalLand@communities.gov.uk",
    license="MIT",
    url="https://github.com/digital-land/data-manager",
    packages=find_packages(exclude="tests"),
    install_requires=open("./requirements/requirements.txt").readlines(),
    dev_requires=open("./requirements/dev-requirements.txt").readlines(),
)
