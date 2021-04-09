"""
Copyright 2019 Zeppelin Bend Pty Ltd
This file is part of ejsonbend.

ejsonbend is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

ejsonbend is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with ejsonbend.  If not, see <https://www.gnu.org/licenses/>.
"""

from setuptools import setup, find_namespace_packages

test_deps = ["pytest", "pytest-cov", "pytest-asyncio", "hypothesis<6"]
setup(
    name="pp-translator",
    version="0.1",
    description="Library to translate zepben.evolve network models to pandapower models",
    url="https://github.com/zepben/pp-translator",
    license="MPL 2.0",
    classifiers=[
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent"
    ],
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src"),
    python_requires='>=3.7',
    install_requires=[
        "zepben.evolve",
        "pandapower"
    ],
    extras_require={
        "test": test_deps,
    }
)
