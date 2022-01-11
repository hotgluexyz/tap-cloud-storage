#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-cloud-storage',
    version='1.0.2',
    description='hotglue tap for importing data from Google Cloud Storage',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_cloud_storage'],
    install_requires=[
        'google-cloud-storage==1.36.2',
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        tap-cloud-storage=tap_cloud_storage:main
    ''',
    packages=['tap_cloud_storage']
)
