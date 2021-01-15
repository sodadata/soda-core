import pathlib
import re
from os import path

from setuptools import find_packages, setup

readme = (pathlib.Path(__file__).parent / "README.md").read_text()

with open("requirements.txt", "r", encoding="utf-8") as file:
    requires = [line.strip() for line in file if line.strip()]

def get_version():
    with open(path.join("sodasql", "__init__.py")) as f:
        contents = f.read()
        matches = re.findall(r"SODA_SQL_VERSION = '([^']+)'", contents)
        return matches[0] if len(matches) > 0 else '__dev__'


setup(
    name="soda-sql",
    version=get_version(),
    author="Tom Baeyens",
    author_email="tom@soda.io",
    description="Soda SQL library & CLI",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    install_requires=requires,
    entry_points={"console_scripts": ["soda=sodasql.cli.cli:main"]},
)
