# -*- coding: utf-8 -*-

""" Appsexception.py. Parsl Application Functions (@) 2021

This module encapsulates all Parsl configuration stuff in order to provide a
cluster configuration based in number of nodes and cores per node.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

from parsl import bash_app, python_app
import parsl

# COPYRIGHT SECTION
__author__ = "Diego Carvalho"
__copyright__ = "Copyright 2021, The Biocomp Informal Collaboration (CEFET/RJ and LNCC)"
__credits__ = ["Diego Carvalho", "Carla Osthoff", "Kary Ocaña", "Rafael Terra"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Diego Carvalho"
__email__ = "d.carvalho@ieee.org"
__status__ = "Research"


#
# Parsl Bash and Python Applications Exceptions
#

class PhylipConversion(Exception):
    """Exception raised for errors in the setup_phylip_data Parsl's bash application.

    Attributes:
        input_dir -- where setup_phylip_data searchs for a tar file
        message -- explanation of the error
    """

    def __init__(self, basedir, message="Unable to convert the NEXUS files to PHYLIP."):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class MrBayesMissingData(Exception):

    def __init__(self, basedir, message="Unable to find the nexus data"):
        self.message = message
        self.basedir = basedir
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class TarMissingData(Exception):
    def __init__(self, basedir, message="Unable to find a tar file with the nexus data."):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class JsonMissingData(Exception):
    def __init__(self, basedir, message="Unable to find the JSON file with root and species mapping"):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class FolderDeletionError(Exception):
    def __init__(self, basedir, message="Unable to delete folder"):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class FolderCreationError(Exception):
    def __init__(self, basedir, message="Unable to create folder"):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class FileCreationError(Exception):
    def __init__(self, basedir, message="Unable to create file"):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'

class RootMissing(Exception):
    def __init__(self, basedir, message="Missing the taxon used as outgroup to root the trees"):
        self.basedir = basedir
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.basedir} -> {self.message}'