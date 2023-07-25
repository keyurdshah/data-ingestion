from setuptools import setup, find_packages

setup(
    name='ingestion-service',
    version='1.0',
    packages=find_packages(),
    install_requires=['pandas', 'pendulum'],
    include_package_data=True,
    entry_points={
        'console_scripts': ['fileingest=fileingestconsole:main'],
    })
