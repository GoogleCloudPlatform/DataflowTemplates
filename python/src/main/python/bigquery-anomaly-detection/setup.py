from setuptools import setup, find_packages

setup(
    name='bqmonitor',
    version='0.1.0',
    description='BigQuery anomaly monitoring pipeline (Dataflow Flex Template)',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.11',
    install_requires=[
        'apache-beam[gcp]>=2.71.0',
        'google-cloud-bigquery-storage',
    ],
)
