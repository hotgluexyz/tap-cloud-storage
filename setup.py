#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-cloud-storage',
    version='1.0.3',
    description='hotglue tap for importing data from Google Cloud Storage',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.14',
    ],
    python_requires='>=3.7',
    py_modules=['tap_cloud_storage'],
    install_requires=[
        'google-cloud-storage>=2.6.0'
    ],
    entry_points='''
        [console_scripts]
        tap-cloud-storage=tap_cloud_storage:main
    ''',
    packages=['tap_cloud_storage']
)
