#!/usr/bin/env python3
from setuptools import setup
import os

setup(
    name='navtiming',
    version=1.0,
    author='Ian Marlier',
    author_email='imarlier@wikimedia.org',
    url='https://wikitech.wikimedia.org/wiki/Webperf',
    license='Apache 2.0',
    description='Client-side metrics processing',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    packages=[
        'navtiming'
    ],
    install_requires=[
        'pyyaml',
        'kafka-python',
        'python-etcd'
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    entry_points={
        'console_scripts': [
            'navtiming = navtiming:main'
        ],
    }
)
