#!/usr/bin/env python3
from setuptools import setup

setup(
    name='navtiming',
    version=1.0,
    author='Ian Marlier',
    author_email='imarlier@wikimedia.org',
    url='https://wikitech.wikimedia.org/wiki/Webperf',
    license='Apache 2.0',
    description='Client-side metrics processing',
    long_description=open('README').read(),
    packages=[
        'navtiming'
    ],
    install_requires=[
        'pyyaml',
        'kafka-python'
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    entry_points={
        'console_scripts': [
            'navtiming = navtiming:main'
        ],
    }
)
