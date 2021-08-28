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

#from apps import quartet_maxcut
from parsl import bash_app, python_app
import parsl, os

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
    execution_provider: str
    network_method:     str
    tree_method:        str
    bootstrap:          str
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
    raxml_dir:          str
    raxml_output:       str
    raxml_threads:      int
    raxml_model:        str
    iqtree:             str
    iqtree_dir:         str
    iqtree_model:       str
    iqtree_threads:     int
    iqtree_output:      str
    astral_exec_dir:    str
    astral_jar:         str
    astral:             str
    astral_dir:         str
    astral_output:      str
    astral_mapping:  str
    snaq:               str
    snaq_threads:       int
    snaq_hmax:          int
    snaq_runs:          int
    snaq_dir:           str
    mrbayes:            str
    mrbayes_parameters: str
    mrbayes_dir:        str
    bucky:              str
    bucky_dir:          str
    mbsum:              str
    mbsum_dir:          str
    quartet_maxcut:     str
    quartet_maxcut_exec_dir: str
    quartet_maxcut_dir: str
    phylonet:           str
    phylonet_exec_dir:  str
    phylonet_jar:       str
    phylonet_threads:   str
    phylonet_hmax:      str
    phylonet_input:     str
    phylonet_dir:       str


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
                dir_ = {}
                if line[0] == '#':
                    continue
                line_with_method = line.split('@')
                dir_['dir'] = line_with_method[0].strip()
                if(len(line_with_method) > 1):
                    methods = line_with_method[1].strip().split('|')
                    if(len(methods) > 0):
                        dir_['tree_method']=methods[0]
                        if(len(methods) > 1):
                            dir_['network_method'] = methods[1]
                        else:
                            dir_['network_method'] = network_method
                    else:
                        dir_['tree_method'] = tree_method
                        dir_['network_method'] = network_method
                else:
                    dir_['tree_method'] = tree_method
                    dir_['network_method'] = network_method
                workload.append(dir_)
        bootstrap = cf['GENERAL']['BootStrap']
        execution_provider = cf['GENERAL']['ExecutionProvider']
        #SYSTEM
        #WORKFLOW
        workflow_name = cf["WORKFLOW"]["Name"]
        workflow_monitor = cf["WORKFLOW"].getboolean("Monitor")
        workflow_part_f = cf["WORKFLOW"]["PartitionFast"]
        workflow_part_t = cf["WORKFLOW"]["PartitionThread"]
        workflow_part_l = cf["WORKFLOW"]["PartitionLong"]
        workflow_wall_t_f = cf["WORKFLOW"]["WalltimeFast"]
        workflow_wall_t_t = cf["WORKFLOW"]["WalltimeThread"]
        workflow_wall_t_l = cf["WORKFLOW"]["WalltimeLong"]
        workflow_core_f = int(cf["WORKFLOW"]["PartCoreFast"])
        workflow_core_t = int(cf["WORKFLOW"]["PartCoreThread"])
        workflow_core_l = int(cf["WORKFLOW"]["PartCoreLong"])
        workflow_node_f = int(cf["WORKFLOW"]["PartNodeFast"])
        workflow_node_t = int(cf["WORKFLOW"]["PartNodeThread"])
        workflow_node_l = int(cf["WORKFLOW"]["PartNodeLong"])
        #RAXML
        raxml = cf['RAXML']['RaxmlExecutable']
        raxml_dir = 'raxml'
        raxml_output = 'besttrees.tre'
        raxml_threads = cf['RAXML']['RaxmlThreads']
        raxml_model = cf['RAXML']['RaxmlEvolutionaryModel']
        #IQTREE
        iqtree = cf['IQTREE']['IqTreeExecutable']
        iqtree_dir = 'iqtree'
        iqtree_model = cf['IQTREE']['IqTreeEvolutionaryModel']
        iqtree_threads = cf['IQTREE']['IqTreeThreads']
        iqtree_output = 'besttrees.tre'
        #ASTRAL
        astral_exec_dir = cf['ASTRAL']['AstralExecDir']
        astral_jar = cf['ASTRAL']['AstralJar']
        astral = f"cd {astral_exec_dir}; java -jar {astral_jar}"
        astral_dir = 'astral'
        astral_output = 'astral.tre'
        astral_mapping = cf['ASTRAL']['AstralMapping']
        #SNAQ
        snaq = cf['SNAQ']['SnaqScript']
        snaq_threads = int(cf['SNAQ']['SnaqThreads'])
        snaq_hmax = int(cf['SNAQ']['SnaqHMax'])
        snaq_runs = int(cf['SNAQ']['SnaqRuns'])
        snaq_dir = 'snaq'
        
        #PHYLONET
        phylonet_exec_dir = cf['PHYLONET']['PhyloNetExecDir']
        phylonet_jar = cf['PHYLONET']['PhyloNetJar']
        phylonet = f"cd {phylonet_exec_dir}; java -jar {phylonet_jar}"
        phylonet_threads = cf['PHYLONET']['PhyloNetThreads']
        phylonet_hmax = cf['PHYLONET']['PhyloNetHMax']
        phylonet_input = 'phylonet_phase_1.nex'
        phylonet_dir = 'phylonet'
        #MRBAYES
        mrbayes = cf['MRBAYES']['MBExecutable']
        mrbayes_parameters = cf['MRBAYES']['MBParameters']
        mrbayes_dir = 'mrbayes'
        #BUCKY
        bucky = cf['BUCKY']['BuckyExecutable']
        bucky_dir= 'bucky'
        #MBSUM
        mbsum = cf['BUCKY']['MbSumExecutable']
        mbsum_dir = 'mbsum'
        #QUARTET MAXCUT
        quartet_maxcut = cf['QUARTETMAXCUT']['QmcExecutable']
        quartet_maxcut_exec_dir = cf['QUARTETMAXCUT']['QmcExecDir']
        quartet_maxcut_dir = 'qmc'
        self.bioconfig = BioConfig(script_dir=script_dir,
                                   workload_path=workload_path,
                                   execution_provider=execution_provider,
                                   network_method=network_method,
                                   tree_method=tree_method,
                                   bootstrap=bootstrap,
                                   workload=workload,
                                   env_path=env_path,
                                   environ=environ,
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
                                   raxml_dir=raxml_dir,
                                   raxml_output=raxml_output,
                                   raxml_threads=raxml_threads,
                                   raxml_model=raxml_model,
                                   iqtree=iqtree,
                                   iqtree_dir=iqtree_dir,
                                   iqtree_model=iqtree_model,
                                   iqtree_threads=iqtree_threads,
                                   iqtree_output=iqtree_output,
                                   astral_exec_dir=astral_exec_dir,
                                   astral_jar=astral_jar,
                                   astral=astral,
                                   astral_dir=astral_dir,
                                   astral_output=astral_output,
                                   astral_mapping=astral_mapping,
                                   snaq=snaq,
                                   snaq_threads=snaq_threads,
                                   snaq_hmax=snaq_hmax,
                                   snaq_runs=snaq_runs,
                                   snaq_dir=snaq_dir,
                                   mrbayes=mrbayes,
                                   mrbayes_parameters=mrbayes_parameters,
                                   mrbayes_dir=mrbayes_dir,
                                   bucky=bucky,
                                   bucky_dir=bucky_dir,
                                   mbsum=mbsum,
                                   mbsum_dir=mbsum_dir,
                                   quartet_maxcut=quartet_maxcut,
                                   quartet_maxcut_exec_dir=quartet_maxcut_exec_dir,
                                   quartet_maxcut_dir=quartet_maxcut_dir,
                                   phylonet=phylonet,
                                   phylonet_exec_dir=phylonet_exec_dir,
                                   phylonet_jar=phylonet_jar,
                                   phylonet_threads=phylonet_threads,
                                   phylonet_hmax=phylonet_hmax,
                                   phylonet_input=phylonet_input,
                                   phylonet_dir=phylonet_dir
                                   )
        return self.bioconfig
