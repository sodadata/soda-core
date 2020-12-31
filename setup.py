from setuptools import setup, find_packages
import pathlib

readme = (pathlib.Path(__file__).parent / "README.md").read_text()

with open("requirements.txt", "r", encoding="utf-8") as file:
    requires = [line.strip() for line in file if line.strip()]

setup(
    name="soda-sql",
    version="0.1.0",
    author="Tom Baeyens",
    author_email="tom@soda.io",
    description="soda-sql library & CLI",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    install_requires=requires,
    entry_points={"console_scripts": ["soda=sodasql.cli:main"]},
)
