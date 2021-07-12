# -*- coding: utf-8 -*-

""" Apps.py. Parsl Application Functions (@) 2021

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
# Parsl Bash and Python Applications
#
import parsl
from bioconfig import BioConfig


# setup_phylip_data bash app


@parsl.python_app(executors=['single_thread'])
def setup_phylip_data(basedir: str, config: BioConfig,
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Convert the gene alignments from the nexus format to the phylip format.

    Parameters:
        cbasedir: it is going to search for a tar file with nexus files. The script will create:
            seqdir=input/nexus
            seqdir=input/phylip
    Returns:
        returns an parsl's AppFuture.

    Raises:
        PhylipMissingData --- if cannot find a tar file with nexus files.


    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import glob
    from appsexception import PhylipMissingData

    input_dir = os.path.join(basedir,'input')
    input_nexus_dir = os.path.join(input_dir, 'nexus')
    # So, some work must be done. Build the Nexus directory
    if not os.path.isdir(input_nexus_dir):
        os.mkdir(input_nexus_dir)
        # List all tar.gz files, they are supposed to be the input
        tar_file_list = glob.glob(f'{input_dir}/*.tar.gz')
        if len(tar_file_list) == 0:
            raise PhylipMissingData(input_dir)
        # So, loop over and untar every file
        for tar_file in tar_file_list:
            os.system(f'cd {input_nexus_dir}; tar zxvf {tar_file}')
    # Now, use the function to convert nexus to phylip.
    import sys
    sys.path.append(config.script_dir)
    import data_management as dm
    dm.nexus_to_phylip(input_nexus_dir) 
    return


# raxml bash app
@parsl.bash_app(executors=['raxml'])
def raxml(basedir: str, config: BioConfig,
          input_file: str,
          inputs=[],
          stderr=parsl.AUTO_LOGNAME,
          stdout=parsl.AUTO_LOGNAME):
    """Runs the Raxml's executable (RPS) on a directory (input)

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import random
    import logging

    num_threads = config.raxml_threads
    raxml_exec = config.raxml
    exec_param = config.raxml_exec_param

    logging.info(f'raxml called with {basedir}')
    raxml_dir = basedir + '/' + config.raxml_dir

    # TODO: Create the following parameters(external configuration): -m, -N,
    flags = f"-T {num_threads} {exec_param}"

    p = random.randint(1, 10000)
    x = random.randint(1, 10000)

    output_file = os.path.basename(input_file).split('.')[0]

    # Return to Parsl to be executed on the workflow
    return f"cd {raxml_dir}; {raxml_exec} {flags} -p {p} -x {x} -s {input_file} -n {output_file}"


@parsl.python_app(executors=['single_thread'])
def setup_tree_output(basedir: str,
                            config: BioConfig,
                            inputs=[],
                            outputs=[],
                            stderr=parsl.AUTO_LOGNAME,
                            stdout=parsl.AUTO_LOGNAME):
    """Create the phylogenetic tree software (raxml, iqtree,...) output file and organize the temporary files to subsequent softwares 

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: 
        Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import sys
    sys.path.append(config.script_dir)
    import data_management as dm
    if(config.tree_method == "ML-RAXML"):
        dm.setup_raxml_output(basedir, config.raxml_dir, config.raxml_output) 
    elif(config.tree_method == "ML-IQTREE"):
        dm.setup_iqtree_output(basedir, config.iqtree_dir, config.iqtree_output)
    return

@parsl.bash_app(executors=['single_thread'])
def astral(basedir: str,
           config: BioConfig,
           inputs=[],
           outputs=[],
           stderr=parsl.AUTO_LOGNAME,
           stdout=parsl.AUTO_LOGNAME):
    """Runs the Astral's executable (RPS) on a directory (input)

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import glob

    astral_dir = f"{basedir}/{config.astral_dir}"
    bs_file = f'{astral_dir}/BSlistfiles'
    boot_strap = f"{basedir}/{config.raxml_dir}/bootstrap/*"

    # Build the invocation command.

    # TODO: manage the fixed bootstrap number...
    num_boot = 100

    # Create bs_file
    with open(bs_file, 'w') as f:
        for i in glob.glob(boot_strap):
            f.write(f'{i}\n')

    exec_astral = config.astral
    raxml_output = f"{basedir}/{config.raxml_output}"
    astral_output = f"{basedir}/{config.astral_output}"

    # Return to Parsl to be executed on the workflow
    return f'{exec_astral} -i {raxml_output} -b {bs_file} -r {num_boot} -o {astral_output}'

@parsl.bash_app(executors=['snaq'])
def snaq(basedir: str,
         config: BioConfig,
         inputs=[],
         outputs=[],
         stderr=parsl.AUTO_LOGNAME,
         stdout=parsl.AUTO_LOGNAME):
    #set environment variables
    import os
    from pathlib import Path
    os.environ["JULIA_SETUP"] = config.julia_setup
    os.environ["JULIA_PKGDIR"] = config.julia_pkgdir
    os.environ["JULIA_SYSIMAGE"] = config.julia_sysimage
    #create snaq folder
    Path(os.path.join(basedir, "snaq")).mkdir(exist_ok=True)
    #run the julia script with PhyloNetworks
    snaq_exec = config.snaq
    num_threads = config.snaq_threads
    hmax = config.snaq_hmax
    if config.tree_method == "ML-RAXML":
        return f'julia {config.julia_sysimage} --threads {num_threads} {snaq_exec} 0 {basedir}/{config.raxml_output} {basedir}/{config.astral_output} {basedir} {num_threads} {hmax}'
    elif config.tree_method == "ML-IQTREE":
        return f'julia {config.julia_sysimage} --threads {num_threads} {snaq_exec} 0 {basedir}/{config.iqtree_output} {basedir}/{config.astral_output} {basedir} {num_threads} {hmax}'
    else:
        pass

# Mr.Bayes bash app
@parsl.bash_app(executors=['single_thread'])
def mrbayes(basedir: str,
            config: BioConfig,
            input_file: str,
            inputs=[],
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
    """Runs the Mr. Bayes' executable (RPS) on a directory (input)

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    from pathlib import Path
    gene_name = os.path.basename(input_file)
    mb_folder = os.path.join(basedir, "mrbayes")
    Path(mb_folder).mkdir(exist_ok=True)
    gene_file = open(input_file, 'r')
    gene_string = gene_file.read()
    gene_file.close()
    gene_par = open(os.path.join(mb_folder, gene_name), 'w+')
    gene_par.write(gene_string)
    par = f"begin mrbayes;\nset nowarnings=yes;\nset autoclose=yes;\nlset nst=2;\n{config.mrbayes_parameters};\nmcmc;\nsumt;\nend;"
    gene_par.write(par)
    return f"{config.MBExecutable} {os.path.join(mb_folder, gene_name)} 2>&1 | tee {os.path.join(mb_folder, gene_name + '.log')}"

# mbsum bash app
@parsl.bash_app(executors=['single_thread'])
def mbsum(basedir: str,
            config: BioConfig,
            input_file: str,
            inputs=[],
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
    """Runs the mbsum' executable (RPS) on a directory (input)

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    from pathlib import Path
    import glob
    gene_name = os.path.basename(input_file)
    mbsum_folder = os.path.join(basedir, "mbsum")
    mrbayes_folder = os.path.join(basedir, "mrbayes")
    Path(mbsum_folder).mkdir(exist_ok=True)
    par = config.mrbayes_parameters.split(' ')
    par_dir = {}
    for p in par:
        P_split = p.split('=')
        par_dir[p[0]] = float(p[1])
    trim =(( (par_dir['ngen']/par_dir['samplefreq'])*par_dir['nruns']*par_dir['burninfrac'])/par_dir['nruns']) +1 
    trees = glob.glob(os.path.join(mrbayes_folder, gene_name + '*.t'))
    return f"mbsum {(' ').join(trees)} -n {trim} -o {os.path.join(mbsum_folder, gene_name + '.sum')}"
# bucky bash app
@parsl.bash_app(executors=['single_thread'])
def bucky(basedir: str,
            config: BioConfig,
            inputs=[],
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
    """Runs the mbsum' executable (RPS) on a directory (input)

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import re
    import os
    from pathlib import Path
    import glob
    from itertools import combinations
    #parse the sumarized taxa by mbsum

    mbsum_folder = os.path.join(basedir, "mbsum")
    files = glob.glob(os.path.join(mbsum_folder, '*.sum'))
    taxa = {}
    selected_taxa = {}
    pattern = re.compile('translate(\n\s*\d+\s+\w+(,|;))+')
    taxa_pattern = re.compile('(\w+(,|;))')
    for file in files:
        gene_sum = open(file, 'r')
        text = gene_sum.read()
        gene_sum.close()
        translate_block = pattern.search(text)
        for match in re.findall(taxa_pattern, translate_block[0]):
            key = re.sub(re.compile('(,|;)'), '',match[0])
            if key in taxa:
                taxa[key]+=1
            else:
                taxa[key]=1
    #select the taxa shared across all genes
    for t in taxa:
        if(taxa[t] == len(files)):
            selected_taxa[t] = taxa[t]
    quartets = combinations(taxa, 4)
    return


@parsl.python_app(executors=['single_thread'])
def setup_phylonet_data(basedir: str,
                      config: BioConfig,
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Get the raxml/iqtree's output and create a NEXUS file as output for the phylonet in the basedir

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    if(config.tree_method == "ML-RAXML"):        
        gene_trees = os.path.join(basedir, config.raxml_dir)
        gene_trees = os.path.join(gene_trees, config.raxml_output)
    else:
        gene_trees = os.path.join(basedir, config.iqtree_dir)
        gene_trees = os.path.join(gene_trees, config.iqtree_output)
    out_dir = os.path.join(basedir,config.phylonet_input)
    import sys
    sys.path.append(config.script_dir)
    import data_management as dm
    dm.create_phylonet_input(gene_trees, out_dir, config.phylonet_hmax, config.phylonet_threads, config.phylonet_threads)
    return
    
@parsl.bash_app(executors=['snaq'])
def phylonet(basedir: str,
         config: BioConfig,
         inputs=[],
         outputs=[],
         stderr=parsl.AUTO_LOGNAME,
         stdout=parsl.AUTO_LOGNAME):
    """Run PhyloNet using as input the phylonet_input variable

    Parameters:
        TODO:
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    exec_phylonet = config.phylonet
    import os
    input_file = os.path.join(basedir,config.phylonet_input)

    # Return to Parsl to be executed on the workflow
    return f'{exec_phylonet} {input_file}'

@parsl.python_app(executors=['single_thread'])
def clear_temporary_files(basedir: str,
                      config: BioConfig,
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    import sys
    import os
    sys.path.append(config.script_dir)
    import data_management as dm
    dm.clear_execution(config.network_method, config.tree_method, basedir)
    return

@parsl.python_app(executors=['single_thread'])
def create_folders(basedir: str,
                      config: BioConfig,
                      folders=[],
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    import os
    import sys
    sys.path.append(config.script_dir)
    import data_management as dm
    dm.create_folders(basedir, folders)
    return
    
@parsl.bash_app(executors=['raxml'])
def iqtree(basedir: str, config: BioConfig,
          input_file: str,
          inputs=[],
          stderr=parsl.AUTO_LOGNAME,
          stdout=parsl.AUTO_LOGNAME):
    import os
    iqtree_dir = os.path.join(basedir, config.iqtree_dir)
    flags = f"-T {config.iqtree_threads} {config.iqtree_exec_param} -s {input_file}"
    # Return to Parsl to be executed on the workflow
    return f"cd {iqtree_dir}; {config.iqtree} {flags}"