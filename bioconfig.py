# -*- coding: utf-8 -*-

""" BioConfig.py. Biocomp Application Configuration (@) 2021

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
__version__ = "1.0.2"
__maintainer__ = "Diego Carvalho"
__email__ = "d.carvalho@ieee.org"
__status__ = "Research"


#
# Parsl Bash and Python Applications Configuration
#
import borg
from dataclasses import dataclass, field

# TODO: self.mbblock = Prepare to read from a setup file.


@dataclass
class BioConfig:
    data_dir:           str
    script_dir:         str
    raxml:              str
    raxml_param:        str
    raxml_phase1:       str
    raxml_dir:          str
    raxml_output:       str
    raxml_threads:      int
    raxml_exec_param:   str
    astral_phase1:      str
    astral_exec_dir:    str
    astral_jar:         str
    astral:             str
    astral_dir:         str
    astral_output:      str
    snaq:               str
    mbblock:            str
    mrbayes:            str


@borg.borg
class ConfigFactory:

    def __init__(self, data_dir: str, config_file: str = 'config/default.ini') -> None:
        import configparser

        self.config_file = config_file
        self.data_dir = data_dir
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

        return

    def build_config(self) -> BioConfig:

        dd = self.data_dir

        script_dir = self.config['GENERAL']['ScriptDir']
        raxml = self.config['RAXML']['RaxmlExecutable']
        raxml_param = self.config['RAXML']['RaxmlParameters']
        raxml_phase1 = f"perl {self.config['RAXML']['RaxmlPhase1']} {raxml_param}"
        raxml_dir = f"{dd}/{self.config['RAXML']['RaxmlDir']}"
        raxml_output = f"{raxml_dir}/{self.config['RAXML']['RaxmlOutput']}"
        raxml_theads = self.config['RAXML']['RaxmlThreads']
        raxml_exec_param = self.config['RAXML']['RaxmlExecParam']
        astral_phase1 = self.config['ASTRAL']['AstralScript']
        astral_exec_dir = self.config['ASTRAL']['AstralExecDir']
        astral_jar = self.config['ASTRAL']['AstralJar']
        astral = f"cd {astral_exec_dir}; java -jar {astral_jar}"
        astral_dir = f"{dd}/{self.config['ASTRAL']['AstralDir']}"
        astral_output = f"{astral_dir}/{self.config['ASTRAL']['AstralOutput']}"
        snaq = f"{script_dir}/{self.config['SNAQ']['SnaqScript']}"
        mbblock = f"{script_dir}/{self.config['MRBAYES']['MrBlock']}"
        mrbayes = f"perl {script_dir}/{self.config['MRBAYES']['MrDriver']}"

        self.bioconfig = BioConfig(self.data_dir,
                                   script_dir,
                                   raxml,
                                   raxml_param,
                                   raxml_phase1,
                                   raxml_dir,
                                   raxml_output,
                                   raxml_theads,
                                   raxml_exec_param,
                                   astral_phase1,
                                   astral_exec_dir,
                                   astral_jar,
                                   astral,
                                   astral_dir,
                                   astral_output,
                                   snaq,
                                   mbblock,
                                   mrbayes)
        return self.bioconfig


class suBioConfig(object):
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

    @property
    def mbblock(self) -> str:
        return f"/scratch/cenapadrjsd/diego.carvalho/biocomp/scripts/mbblock.txt"

    @property
    def mrbayes(self) -> str:
        return f"perl {self.script_dir}/mb.pl"
