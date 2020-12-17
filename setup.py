from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as f:
    requires = [x.strip() for x in f if x.strip()]

setup(
    name="sodasql",
    version="0.1.0",
    author="Tom Baeyens",
    author_email="tom@soda.io",
    description="SodaSQL library & CLI",
    packages=find_packages(exclude=["sodasql_*"]),
    install_requires=requires,
    entry_points={"console_scripts": ["sodasql=sodasql.main:main"]},
)
