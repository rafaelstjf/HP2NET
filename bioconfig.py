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
from dataclasses import dataclass, field

# TODO: self.mbblock = Prepare to read from a setup file.


class borg(object):
    def __init__(self, my_class):
        self.my_class = my_class
        self.my_instance = None

    def __call__(self, *args, **kwargs):
        if self.my_instance == None:
            self.my_instance = self.my_class(*args, **kwargs)
        return self.my_instance


@dataclass
class BioConfig:
    env_path:           str
    environ:            str
    script_dir:         str
    workload_path:      str
    network_method:     str
    tree_method:        str
    workload:           field(default_factory=list)
    workflow_name:      str
    workflow_monitor:   bool
    workflow_part_f:    str
    workflow_part_t:    str
    workflow_part_l:    str
    workflow_wall_t_f:  str
    workflow_wall_t_t:  str
    workflow_wall_t_l:  str
    workflow_core_f:    int
    workflow_core_t:    int
    workflow_core_l:    int
    workflow_node_f:    int
    workflow_node_t:    int
    workflow_node_l:    int
    raxml:              str
    raxml_param:        str
    raxml_dir:          str
    raxml_output:       str
    raxml_threads:      int
    raxml_exec_param:   str
    iqtree:             str
    iqtree_dir:         str
    iqtree_exec_param:  str
    iqtree_threads:     int
    iqtree_output:      str
    astral_exec_dir:    str
    astral_jar:         str
    astral:             str
    astral_dir:         str
    astral_output:      str
    snaq:               str
    snaq_threads:       int
    mbblock:            str
    mrbayes:            str
    phylonet:           str
    phylonet_exec_dir:  str
    phylonet_jar:       str
    phylonet_threads:   str
    phylonet_hmax:      str
    phylonet_input:     str


@borg
class ConfigFactory:

    def __init__(self, config_file: str = "config/default.ini") -> None:
        import configparser

        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

        return

    def build_config(self) -> BioConfig:

        cf = self.config
        script_dir = cf['GENERAL']['ScriptDir']

        env_path = cf['GENERAL']['Environ']
        environ = ""  # empty
        with open(f"{env_path}", "r") as f:
            environ = f.read()
        #Choose which method is going to be used to construct the network (Phylonet, SNAQ and others)
        network_method = cf['GENERAL']['NetworkMethod']
        tree_method = cf['GENERAL']['TreeMethod']
        # Read where datasets are...
        workload_path = cf['GENERAL']['Workload']
        workload = list()
        with open(f"{workload_path}", "r") as f:
            for line in f:
                if line[0] == '#':
                    continue
                workload.append(line.strip())

        perl_int = cf['SYSTEM']['PerlInter']
        workflow_name = cf["WORKFLOW"]["Name"]
        workflow_monitor = cf["WORKFLOW"].getboolean("Monitor")
        workflow_part_f = cf["WORKFLOW"]["PartitionFast"]
        workflow_part_t = cf["WORKFLOW"]["PartitionThread"]
        workflow_part_l = cf["WORKFLOW"]["PartitionLong"]
        workflow_wall_t_f = cf["WORKFLOW"]["WalltimeFast"]
        workflow_wall_t_t = cf["WORKFLOW"]["WalltimeThread"]
        workflow_wall_t_l = cf["WORKFLOW"]["WalltineLong"]
        workflow_core_f = int(cf["WORKFLOW"]["PartCoreFast"])
        workflow_core_t = int(cf["WORKFLOW"]["PartCoreThread"])
        workflow_core_l = int(cf["WORKFLOW"]["PartCoreLong"])
        workflow_node_f = int(cf["WORKFLOW"]["PartNodeFast"])
        workflow_node_t = int(cf["WORKFLOW"]["PartNodeThread"])
        workflow_node_l = int(cf["WORKFLOW"]["PartNodeLong"])

        raxml = cf['RAXML']['RaxmlExecutable']
        raxml_param = cf['RAXML']['RaxmlParameters']
        raxml_dir = cf['RAXML']['RaxmlDir']
        raxml_output = f"{raxml_dir}/{cf['RAXML']['RaxmlOutput']}"
        raxml_threads = cf['RAXML']['RaxmlThreads']
        raxml_exec_param = cf['RAXML']['RaxmlExecParam']

        iqtree = cf['IQTREE']['IqTreeExecutable']
        iqtree_dir = cf['IQTREE']['IqTreeDir']
        iqtree_exec_param = cf['IQTREE']['IqTreeParameters']
        iqtree_threads = cf['IQTREE']['IqTreeThreads']
        iqtree_output = cf['IQTREE']['iqTreeOutput']

        astral_exec_dir = cf['ASTRAL']['AstralExecDir']
        astral_jar = cf['ASTRAL']['AstralJar']
        astral = f"cd {astral_exec_dir}; java -jar {astral_jar}"
        astral_dir = cf['ASTRAL']['AstralDir']
        astral_output = f"{astral_dir}/{cf['ASTRAL']['AstralOutput']}"

        snaq = f"{script_dir}/{cf['SNAQ']['SnaqScript']}"
        snaq_threads = int(cf['SNAQ']['SnaqThreads'])

        mbblock = ""  # empty
        with open(f"{script_dir}/{cf['MRBAYES']['MrBlock']}", "r") as f:
            mbblock = f.read()
        mrbayes = f"{perl_int} {script_dir}/{cf['MRBAYES']['MrDriver']}"

        phylonet_exec_dir = cf['PHYLONET']['PhyloNetExecDir']
        phylonet_jar = cf['PHYLONET']['PhyloNetJar']
        phylonet = f"cd {phylonet_exec_dir}; java -jar {phylonet_jar}"
        phylonet_threads = cf['PHYLONET']['PhyloNetThreads']
        phylonet_hmax = cf['PHYLONET']['PhyloNetHMax']
        phylonet_input = cf['PHYLONET']['PhyloNetInput']

        self.bioconfig = BioConfig(script_dir=script_dir,
                                   workload_path=workload_path,
                                   network_method=network_method,
                                   tree_method=tree_method,
                                   workload=workload,
                                   env_path=env_path,
                                   environ=environ,
                                   script_dir=script_dir,
                                   workflow_monitor=workflow_monitor,
                                   workflow_name=workflow_name,
                                   workflow_part_f=workflow_part_f,
                                   workflow_part_t=workflow_part_t,
                                   workflow_part_l=workflow_part_l,
                                   workflow_wall_t_f=workflow_wall_t_f,
                                   workflow_wall_t_t=workflow_wall_t_t,
                                   workflow_wall_t_l=workflow_wall_t_l,
                                   workflow_core_f=workflow_core_f,
                                   workflow_core_t=workflow_core_t,
                                   workflow_core_l=workflow_core_l,
                                   workflow_node_f=workflow_node_f,
                                   workflow_node_t=workflow_node_t,
                                   workflow_node_l=workflow_node_l,
                                   raxml=raxml,
                                   raxml_param=raxml_param,
                                   raxml_dir=raxml_dir,
                                   raxml_output=raxml_output,
                                   raxml_threads=raxml_threads,
                                   raxml_exec_param=raxml_exec_param,
                                   iqtree=iqtree,
                                   iqtree_dir=iqtree_dir,
                                   iqtree_exec_param=iqtree_exec_param,
                                   iqtree_threads=iqtree_threads,
                                   iqtree_output=iqtree_output,
                                   astral_exec_dir=astral_exec_dir,
                                   astral_jar=astral_jar,
                                   astral=astral,
                                   astral_dir=astral_dir,
                                   astral_output=astral_output,
                                   snaq=snaq,
                                   snaq_threads=snaq_threads,
                                   mbblock=mbblock,
                                   mrbayes=mrbayes,
                                   phylonet=phylonet,
                                   phylonet_exec_dir=phylonet_exec_dir,
                                   phylonet_jar=phylonet_jar,
                                   phylonet_threads=phylonet_threads,
                                   phylonet_hmax=phylonet_hmax,
                                   phylonet_input=phylonet_input)
        return self.bioconfig
