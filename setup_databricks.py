# W setup_databricks.py
from setuptools import setup, find_packages # WAŻNE: Importuj find_packages

setup(
    name='cv-demo-databricks',
    version='0.1.0',
    packages=find_packages(where='src'), # Znajdzie pakiety w katalogu 'src'
    package_dir={'': 'src'}, # Powiedz setuptools, że pakiety zaczynają się w 'src'
    install_requires=[
        'pyspark>=3.3.0',
        'pandas>=2.0.0',
        # Ważne: Dodaj tutaj swój pakiet `decorators` jako zależność!
        # Jeśli to osobny pakiet wheel, możesz potrzebować go zainstalować osobno
        # 'cv-demo-decorators',
        # Dodaj WSZYSTKIE inne biblioteki Pythona, których używa KOD W 'azure_databricks'
    ],
    python_requires='>=3.9',
    author='Your Name',
    description='Core Databricks utilities for CV-DEMO1 project.',
)