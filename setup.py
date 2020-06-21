#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages
import stopover

setup(name='stopover',
      version=stopover.__version__,
      description="Stopover's Python SDK",
      url='',
      author='Rodrigo Martínez Castaño',
      author_email='rodrigo@martinez.gal',
      license='GNU General Public License v3 (GPLv3)',
      packages=find_packages(),
      zip_safe=False,
      classifiers=[
          "Development Status :: 4 - Beta",
          "Environment :: Console",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
          "Operating System :: POSIX :: Linux",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: Implementation :: PyPy",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      python_requires=">=3.6",
      install_requires=['bson==0.5.10', 'python-snappy==0.5.4', 'requests==2.24.0'])  # Dependencies
