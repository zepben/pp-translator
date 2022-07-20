#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import setup, find_packages

test_deps = ["pytest", "pytest-cov", "pytest-asyncio", "hypothesis<6"]
setup(
    name="pp-translator",
    description="Library for translating Zepben CIM network models to pandapower models",
    version="0.5.0b2",
    url="https://github.com/zepben/pp-translator",
    author="Zeppelin Bend",
    author_email="oss@zepben.com",
    license="MPL 2.0",
    classifiers=[
         "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
         "Programming Language :: Python :: 3",
         "Programming Language :: Python :: 3.7",
         "Programming Language :: Python :: 3.8",
         "Programming Language :: Python :: 3.9",
         "Programming Language :: Python :: 3.10",
         "Operating System :: OS Independent"
     ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires='>=3.7',
    py_modules=[splitext(basename(path))[0] for path in glob('src/**/*.py')],
    install_requires=[
        "zepben.evolve==0.28.0b11",
        "pandapower==2.9.0"
    ],
    extras_require={
        "test": test_deps,
    },
)
