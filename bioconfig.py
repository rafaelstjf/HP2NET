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
# Parsl Bash and Python Applications Configuration
#

class BioConfig(object):
    def __init__(self,
                 script_dir='/scratch/cenapadrjsd/diego.carvalho/biocomp/scripts/') -> None:
        self.script_dir = script_dir
        return

    @property
    def raxml(self) -> str:
        return 'raxmlHPC-PTHREADS'

    @property
    def raxml_phase1(self) -> str:
        return f"perl {self.script_dir}/raxml-phase1.pl --seqdir=input/nexus --raxmldir=raxml --astraldir=astral"

    def raxml_dir(self, datadir) -> str:
        return f'{datadir}/raxml'

    def raxml_output(self, datadir) -> str:
        return f'{datadir}/raxml/besttrees.tre'

    @property
    def astral_phase1(self) -> str:
        return f"{self.script_dir}/setup_astral_data.sh"

    @property
    def astral(self) -> str:
        astral_exec_dir = '/scratch/cenapadrjsd/diego.carvalho/biocomp/Astral/'
        astral_jar = 'astral.5.7.4.jar'

        return f"cd {astral_exec_dir}; java -jar {astral_jar}"

    def astral_dir(self, datadir) -> str:
        return f'{datadir}/astral'

    def astral_output(self, datadir) -> str:
        return f'{datadir}/astral/astral.tre'

    @property
    def snaq(self) -> str:
        return f"/scratch/cenapadrjsd/diego.carvalho/biocomp/scripts/snaq.sh"
