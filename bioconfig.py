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

import configparser
import glob
import json
import os
import random
from dataclasses import dataclass, field

from appsexception import JsonMissingData, RootMissing, TarMissingData

# COPYRIGHT SECTION
__author__ = "Diego Carvalho"
__copyright__ = "Copyright 2021, The Biocomp Informal Collaboration (CEFET/RJ and LNCC)"
__credits__ = ["Diego Carvalho", "Carla Osthoff", "Kary Ocaña", "Rafael Terra"]
__license__ = "GPL"
__version__ = "1.0.2"
__maintainer__ = "Rafael Terra"
__email__ = "rafaelst@posgrad.lncc.br"
__status__ = "Research"


# TODO: self.mbblock = Prepare to read from a setup file.

class borg(object):
    def __init__(self, my_class):
        self.my_class = my_class
        self.my_instance = None

    def __call__(self, *args, **kwargs):
        if self.my_instance == None:
            self.my_instance = self.my_class(*args, **kwargs)
        return self.my_instance


@dataclass()
class BioConfig:
    env_path:           str
    environ:            str
    script_dir:         str
    execution_provider: str
    plot_networks:      bool
    network_method:     str
    tree_method:        str
    bootstrap:          str
    seed:               int
    workload:           field(default_factory=list)
    workflow_name:      str
    workflow_path:      str
    workflow_monitor:   bool
    workflow_walltime:  str
    workflow_core:    int
    workflow_node:    int
    raxml:              str
    raxml_dir:          str
    raxml_output:       str
    raxml_rooted_output: str
    raxml_threads:      int
    raxml_model:        str
    iqtree:             str
    iqtree_dir:         str
    iqtree_model:       str
    iqtree_threads:     int
    iqtree_output:      str
    iqtree_rooted_output: str
    astral_exec_dir:    str
    astral_jar:         str
    astral:             str
    astral_dir:         str
    astral_output:      str
    snaq:               str
    snaq_threads:       int
    snaq_hmax:          field(default_factory=list)
    snaq_runs:          int
    snaq_dir:           str
    mrbayes:            str
    mrbayes_evolutionary_model: str
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
    phylonet_hmax:      field(default_factory=list)
    phylonet_input:     str
    phylonet_dir:       str
    phylonet_runs:      str
    plot_script:        str

    def __hash__(self):
        workload_tuples = [tuple(item.items()) for item in self.workload]
        return hash((
            self.env_path,
            self.environ,
            self.script_dir,
            self.execution_provider,
            self.plot_networks,
            self.network_method,
            self.tree_method,
            self.bootstrap,
            self.seed,
            tuple(workload_tuples),
            self.workflow_name,
            self.workflow_path,
            self.workflow_monitor,
            self.workflow_walltime,
            self.workflow_core,
            self.workflow_node,
            self.raxml,
            self.raxml_dir,
            self.raxml_output,
            self.raxml_rooted_output,
            self.raxml_threads,
            self.raxml_model,
            self.iqtree,
            self.iqtree_dir,
            self.iqtree_model,
            self.iqtree_threads,
            self.iqtree_output,
            self.iqtree_rooted_output,
            self.astral_exec_dir,
            self.astral_jar,
            self.astral,
            self.astral_dir,
            self.astral_output,
            self.snaq,
            self.snaq_threads,
            tuple(self.snaq_hmax),
            self.snaq_runs,
            self.snaq_dir,
            self.mrbayes,
            self.mrbayes_evolutionary_model,
            self.mrbayes_parameters,
            self.mrbayes_dir,
            self.bucky,
            self.bucky_dir,
            self.mbsum,
            self.mbsum_dir,
            self.quartet_maxcut,
            self.quartet_maxcut_exec_dir,
            self.quartet_maxcut_dir,
            self.phylonet,
            self.phylonet_exec_dir,
            self.phylonet_jar,
            self.phylonet_threads,
            tuple(self.phylonet_hmax),
            self.phylonet_input,
            self.phylonet_dir,
            self.phylonet_runs,
            self.plot_script,
        ))


def _map_mrbayes_model(model: str) -> str:
    models = {
        "hky": "nst=2",
        "hky + gamma": "nst=2 rates=gamma",
        "hky + invariant sites": "nst=2 rates=propinv",
        "hky + invariant sites + gamma": "nst=2 rates=invgamma",
        "gtr": "nst=6",
        "gtr + gamma": "nst=6 rates=gamma",
        "gtr + invariant sites": "nst=6 rates=propinv",
        "gtr + invariant sites + gamma": "nst=6 rates=invgamma",
    }
    normalized_model = " ".join(model.casefold().split())
    try:
        return models[normalized_model]
    except KeyError as error:
        raise ValueError(
            f"Unsupported MrBayes evolutionary model '{model}'. "
            f"Choose one of: {', '.join(models)}."
        ) from error


def _read_workload_entry(line: str, network_method: str, tree_method: str) -> dict:
    workload_item = {}
    line_with_method = line.split("@")
    workload_item["dir"] = line_with_method[0].strip()

    if len(line_with_method) > 1:
        methods = line_with_method[1].strip().split("|")
        workload_item["tree_method"] = methods[0]
        if len(methods) > 1:
            workload_item["network_method"] = methods[1]
        else:
            workload_item["network_method"] = network_method
    else:
        workload_item["tree_method"] = tree_method
        workload_item["network_method"] = network_method

    input_dir = os.path.join(workload_item["dir"], "input")
    if not os.path.isdir(input_dir):
        input_dir = os.path.join(workload_item["dir"], "Input")

    json_files = glob.glob(os.path.join(input_dir, "*.json"))
    if not json_files:
        raise JsonMissingData(workload_item["dir"])

    json_path = json_files[0]
    with open(json_path, "r") as json_file:
        try:
            json_data = json.load(json_file)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"Invalid JSON in {json_path} at line "
                f"{error.lineno}, column {error.colno}: {error.msg}"
            ) from error

    if not isinstance(json_data, dict):
        raise ValueError(
            f"Invalid input JSON in {json_path}: "
            "the top-level value must be an object"
        )
    for key in ("Mapping", "Outgroup"):
        if key not in json_data:
            raise ValueError(
                f"Invalid input JSON in {json_path}: "
                f"missing required field '{key}'"
            )
        if not isinstance(json_data[key], str):
            raise ValueError(
                f"Invalid input JSON in {json_path}: "
                f"field '{key}' must be a string"
            )

    workload_item["mapping"] = json_data["Mapping"]
    workload_item["outgroup"] = json_data["Outgroup"].strip()
    if not workload_item["outgroup"] and workload_item["network_method"] == "MP":
        raise RootMissing(workload_item["dir"])

    tar_files = glob.glob(os.path.join(input_dir, "*.tar.gz"))
    if not tar_files:
        raise TarMissingData(workload_item["dir"])
    workload_item["sequences"] = tar_files[0]
    return workload_item


def _load_workload(
    workload_path: str, network_method: str, tree_method: str
) -> list:
    workload = []
    with open(workload_path, "r") as workload_file:
        for line in workload_file:
            line = line.strip()
            if line and not line.startswith("#"):
                workload.append(
                    _read_workload_entry(line, network_method, tree_method)
                )
    return workload


@borg
class ConfigFactory:
    def __init__(self, config_file: str = "default.ini", custom_workload: str = None) -> None:
        self.custom_workload = custom_workload
        self.config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.path.join('config', config_file))
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        return
    def build_config(self) -> BioConfig:

        cf = self.config
        workflow_path = os.path.dirname(os.path.realpath(__file__))
        script_dir = os.path.join(workflow_path, 'scripts')
        env_path = os.path.join(workflow_path, os.path.join('config', cf['GENERAL']['Environ']))
        environ = ""  # empty
        with open(f"{env_path}", "r") as f:
            environ = f.read()
            environ+=f'\nexport PYTHONPATH=$PYTHONPATH:{workflow_path}'
        #Choose which method is going to be used to construct the network (Phylonet, SNAQ and others)
        network_method = cf['GENERAL']['NetworkMethod']
        tree_method = cf['GENERAL']['TreeMethod']
        # Read where datasets are...
        if self.custom_workload is not None:
            workload_path = os.path.join(workflow_path, os.path.join('config', self.custom_workload))
        else:
            workload_path = os.path.join(workflow_path, os.path.join('config', cf['GENERAL']['Workload']))
        workload = _load_workload(workload_path, network_method, tree_method)
        bootstrap = cf['GENERAL']['BootStrap']
        execution_provider = cf['GENERAL']['ExecutionProvider'].upper()
        plot_networks = cf["WORKFLOW"].getboolean("Plot")
        seed = int(cf["GENERAL"].get("Seed", random.randrange(1, 1000, 2)))
        #SYSTEM
        #WORKFLOW
        workflow_name = "HP2NET"
        workflow_monitor = cf["WORKFLOW"].getboolean("Monitor")
        if execution_provider == "SLURM":
            workflow_walltime = cf["WORKFLOW"]["Walltime"]
            workflow_core = int(cf["WORKFLOW"]["PartCore"]) #hardcoded to ensure a free core to parsl 
            workflow_node = int(cf["WORKFLOW"]["PartNode"])
        else:
            workflow_walltime = None
            workflow_core = int(cf["WORKFLOW"]["MaxCore"]) #hardcoded to ensure a free core to parsl 
            workflow_node = int(cf["WORKFLOW"]["CoresPerWorker"])
        #RAXML
        raxml = cf['RAXML']['RaxmlExecutable']
        raxml_dir = 'raxml'
        raxml_output = 'besttrees.tre'
        raxml_rooted_output = 'besttrees_rooted.tre'
        raxml_threads = cf['RAXML']['RaxmlThreads']
        raxml_model = cf['RAXML']['RaxmlEvolutionaryModel']
        #IQTREE
        iqtree = cf['IQTREE']['IqTreeExecutable']
        iqtree_dir = 'iqtree'
        iqtree_model = cf['IQTREE']['IqTreeEvolutionaryModel']
        iqtree_threads = cf['IQTREE']['IqTreeThreads']
        iqtree_output = 'besttrees.tre'
        iqtree_rooted_output = 'besttrees_rooted.tre'
        #ASTRAL
        astral_exec_dir = cf['ASTRAL']['AstralExecDir']
        astral_jar = cf['ASTRAL']['AstralJar']
        astral = f"java -jar {os.path.join(astral_exec_dir, astral_jar)}"
        astral_dir = 'astral'
        astral_output = 'astral.tre'
        #SNAQ
        snaq = 'snaq.jl'
        snaq_threads = int(cf['SNAQ']['SnaqThreads'])
        snaq_hmax_raw = cf['SNAQ']['SnaqHMax']
        snaq_hmax = [h.strip() for h in snaq_hmax_raw.split(',') if h.strip()]
        if any(int(h) < 0 for h in snaq_hmax):
            raise ValueError("SNAQ SnaqHMax values cannot be negative")
        if len(snaq_hmax) == 1:
            max_h = int(snaq_hmax[0])
            snaq_hmax = (
                ["0"] if max_h == 0 else [str(h) for h in range(1, max_h + 1)]
            )
        snaq_runs = int(cf['SNAQ']['SnaqRuns'])
        snaq_dir = 'snaq'
        
        #PHYLONET
        phylonet_exec_dir = cf['PHYLONET']['PhyloNetExecDir']
        phylonet_jar = cf['PHYLONET']['PhyloNetJar']
        phylonet = f"java -jar {os.path.join(phylonet_exec_dir, phylonet_jar)}"
        phylonet_threads = cf['PHYLONET']['PhyloNetThreads']
        phylonet_runs = cf['PHYLONET']['PhyloNetRuns']
        phylonet_hmax_raw = cf['PHYLONET']['PhyloNetHMax']
        phylonet_hmax = [h.strip() for h in phylonet_hmax_raw.split(',') if h.strip()]
        if any(int(h) < 0 for h in phylonet_hmax):
            raise ValueError("PhyloNet PhyloNetHMax values cannot be negative")
        if len(phylonet_hmax) == 1:
            max_h = int(phylonet_hmax[0])
            phylonet_hmax = (
                ["0"] if max_h == 0 else [str(h) for h in range(1, max_h + 1)]
            )
        phylonet_input = 'phylonet_phase_1.nex'
        phylonet_dir = 'phylonet'
        #MRBAYES
        mrbayes = cf['MRBAYES']['MBExecutable']
        mrbayes_model = cf['MRBAYES']['MBEvolutionaryModel']
        mrbayes_evolutionary_model = _map_mrbayes_model(mrbayes_model)
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
        #PLOT SCRIPT
        plot_script = os.path.join(script_dir, "plot.jl")
        self.bioconfig = BioConfig(script_dir=script_dir,
                                   execution_provider=execution_provider,
                                   plot_networks=plot_networks,
                                   network_method=network_method,
                                   tree_method=tree_method,
                                   bootstrap=bootstrap,
                                   seed=seed,
                                   workload=workload,
                                   env_path=env_path,
                                   environ=environ,
                                   workflow_monitor=workflow_monitor,
                                   workflow_name=workflow_name,
                                   workflow_path=workflow_path,
                                   workflow_walltime=workflow_walltime,
                                   workflow_core=workflow_core,
                                   workflow_node=workflow_node,
                                   raxml=raxml,
                                   raxml_dir=raxml_dir,
                                   raxml_output=raxml_output,
                                   raxml_rooted_output=raxml_rooted_output,
                                   raxml_threads=raxml_threads,
                                   raxml_model=raxml_model,
                                   iqtree=iqtree,
                                   iqtree_dir=iqtree_dir,
                                   iqtree_model=iqtree_model,
                                   iqtree_threads=iqtree_threads,
                                   iqtree_output=iqtree_output,
                                   iqtree_rooted_output=iqtree_rooted_output,
                                   astral_exec_dir=astral_exec_dir,
                                   astral_jar=astral_jar,
                                   astral=astral,
                                   astral_dir=astral_dir,
                                   astral_output=astral_output,
                                   snaq=snaq,
                                   snaq_threads=snaq_threads,
                                   snaq_hmax=snaq_hmax,
                                   snaq_runs=snaq_runs,
                                   snaq_dir=snaq_dir,
                                   mrbayes=mrbayes,
                                   mrbayes_evolutionary_model=mrbayes_evolutionary_model,
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
                                   phylonet_dir=phylonet_dir,
                                   phylonet_runs=phylonet_runs,
                                   plot_script=plot_script
                                   )
        return self.bioconfig
