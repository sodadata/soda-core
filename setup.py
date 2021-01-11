import re

from setuptools import setup, find_packages
import pathlib

readme = (pathlib.Path(__file__).parent / "README.md").read_text()

with open("requirements.txt", "r", encoding="utf-8") as file:
    requires = [line.strip() for line in file if line.strip()]


def get_version():
    with open('sodasql/__init__.py') as f:
        init_contents = f.read()
        return re.match(r"SODA_SQL_VERSION = '([^']+)'", init_contents).group(0)


setup(
    name="soda-sql",
    version=get_version(),
    author="Tom Baeyens",
    author_email="tom@soda.io",
    description="Soda-SQL library & CLI",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    install_requires=requires,
    entry_points={"console_scripts": ["soda=sodasql.cli.cli:main"]},
)
