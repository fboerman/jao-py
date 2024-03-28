# -*- coding: utf-8 -*-

"""
A setuptools based setup module for jao-py

Adapted from
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the source code
with open(path.join(here, 'jao', 'jao.py'), encoding='utf-8') as f:
    lines = f.readlines()
    for l in lines:
        if l.startswith('__version__'):
            __version__ = l.split('"')[1]

setup(
    name='jao-py',
    version=__version__,
    description='A python API wrapper for JAO.eu',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/fboerman/jao-py',
    author='Frank Boerman',
    author_email='frank@fboerman.nl',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],

    keywords='JAO data api energy',

    packages=find_packages(),

    install_requires=['requests', 'pandas', 'python-dateutil', 'beautifulsoup4'],

    package_data={
        'jao-py': ['LICENSE.md', 'README.md'],
    },
)
