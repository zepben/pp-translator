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


from glob import glob
from os.path import basename
from os.path import splitext
from setuptools import setup, find_packages


test_deps = ["pytest"]
setup(
    name="pp-translator",
    version="0.1",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    install_requires=[
        "zepben.evolve==0.21.0",
        "pydash",
        "geopandas"
    ],
    extras_require={
        "test": test_deps,
    },
)
