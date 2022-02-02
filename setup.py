#!/usr/bin/env python

from os import path
import ast
import re

try:
    from setuptools import setup
    extra = dict(include_package_data=True)
except ImportError:
    from distutils.core import setup
    extra = {}

BASE_DIR = path.abspath(path.dirname(__file__))
with open(path.join(BASE_DIR, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    "aiohttp",
    "requests",
    "requests-aws",
]

_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('aiorgwadmin/__init__.py', encoding='utf-8') as f:
    version = str(ast.literal_eval(_version_re.search(f.read()).group(1)))


setup(
    name="aiorgwadmin",
    packages=["aiorgwadmin"],
    package_data={"aiorgwadmin": ["py.typed"]},
    zip_safe=False,
    version=version,
    install_requires=install_requires,
    author="Derek Yarnell <derek@umiacs.umd.edu>, Mikle Green",
    url="https://github.com/mikle-green/aiorgwadmin",
    license="LGPL v2.1",
    description="Python Rados Gateway Admin API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=["ceph", "radosgw", "admin api", "async"],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.6",
    **extra
)
