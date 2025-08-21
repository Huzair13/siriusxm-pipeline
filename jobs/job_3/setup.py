
from setuptools import setup, find_packages

setup(
    name='utils',
    version='0.1',
    packages=find_packages(),
    description='Glue utils for job_3',
    include_package_data=True,
    package_data={'utils': ['vendor/**/*']},
)
