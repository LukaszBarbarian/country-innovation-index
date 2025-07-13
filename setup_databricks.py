# CV-DEMO1/setup_databricks.py

from setuptools import setup

setup(
    name='cv-demo-databricks', # Unikalna nazwa pakietu do instalacji (np. pip install cv-demo-databricks)
    version='0.1.0',
    packages=['azure_databricks'], # Jawnie pakujemy tylko ten główny pakiet
    package_dir={'azure_databricks': 'src/azure_databricks'}, # Mapuje go do src/azure_databricks
    install_requires=[
        'pyspark>=3.3.0',
        'pandas>=2.0.0',
        # Ważne: Dodaj tutaj swój pakiet `decorators` jako zależność!
        # Python wie, że musi go najpierw zainstalować.
       # 'cv-demo-decorators', # Nazwa pakietu z setup_decorators.py
        # Dodaj WSZYSTKIE inne biblioteki Pythona, których używa KOD W 'azure_databricks'
    ],
    python_requires='>=3.9', # Minimalna wersja Pythona
    author='Your Name',
    description='Core Databricks utilities for CV-DEMO1 project.',
)