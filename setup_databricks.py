# W setup_databricks.py
from setuptools import setup, find_packages

setup(
    name='cv-demo-databricks',
    version='0.1.0',
    packages=find_packages(where='src'), 
    package_dir={'': 'src'}, 
    install_requires=[
        'pyspark>=3.3.0',
        'pandas>=2.0.0',
    ],
    python_requires='>=3.9',
    author='Your Name',
    description='Core Databricks utilities for CV-DEMO1 project.',
)