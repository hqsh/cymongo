#!/usr/bin/env python
from __future__ import print_function
from setuptools import setup, find_packages
import textwrap


setup(
    name='cymongo',
    version='0.1',
    url='',
    author='Huang Qiangsheng',
    author_email='hqsh@live.cn',
    description="Much faster than pymongo.",
    long_description=textwrap.dedent(""),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    packages=find_packages(),
    py_modules=('cymongo', ),
    install_requires=['numpy', 'pandas']
)
