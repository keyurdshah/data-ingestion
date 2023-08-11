from setuptools import setup, find_packages
print (find_packages())
setup(
    name='data-ingestion-common',
    version='1.0',
    packages=find_packages(exclude=['tests*']),
    install_requires=['pandas', 'pendulum'],
    include_package_data=True,
)
