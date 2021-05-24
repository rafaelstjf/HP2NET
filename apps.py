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


@parsl.bash_app(executors=['single_thread'])
def setup_phylip_data(basedir: str, config: BioConfig,
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Convert the nexus format to phylip in order to run Raxml on a directory.

    Parameters:
        config: BioConfig --- config.data_dir is the directory where raxml-phase1 
        is going to search for a tar file with nexus files. The script will create:
            seqdir=input/nexus
            raxmldir=raxml
            astraldir=astral
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

    raxml_phase1 = config.raxml_phase1

    input_dir = basedir + '/input'
    input_nexus_dir = input_dir + '/nexus'
    phylip_dir = input_dir + '/phylip'

    # If we find phylip_dir, we suppose the input is Ok.
    if not os.path.isfile(phylip_dir):

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

    # Now, use the modified script to convert nexus to phylip.
    return f'cd {basedir}; {raxml_phase1}'


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


@parsl.bash_app(executors=['single_thread'])
def setup_astral_data(basedir: str,
                      config: BioConfig,
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):

    astral_phase1 = config.astral_phase1
    raxml_dir = f"{basedir}/{config.raxml_dir}"

    return f'{astral_phase1} {raxml_dir}'


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


# TODO: Export the parameter hmax (in .jl)
@parsl.bash_app(executors=['snaq'])
def snaq(basedir: str,
         config: BioConfig,
         inputs=[],
         outputs=[],
         stderr=parsl.AUTO_LOGNAME,
         stdout=parsl.AUTO_LOGNAME):

    snaq_exec = config.snaq
    num_threads = config.snaq_threads
    return f'{snaq_exec} {basedir} {num_threads}'


# Mr.Bayes bash app
@parsl.bash_app(executors=['raxml'])
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
    import random
    import logging
    import bioconfig

    c = bioconfig.BioConfig()

    logging.info(f'mrbayes called with {basedir}')
    mrbayes_dir = f"{basedir}/mrbayes"

    # If we find phylip_dir, we suppose the input is Ok.
    if not os.path.isdir(mrbayes_dir):
        os.mkdir(mrbayes_dir)

    p = random.randint(1, 10000)
    x = random.randint(1, 10000)

    output_file = os.path.basename(input_file).split('.')[0]

    # Return to Parsl to be executed on the workflow
    return f"cd {mrbayes_dir}; {c.raxml} -p {p} -x {x} -s {input_file} -n {output_file}"

@parsl.python_app(executors=['single_thread'])
def setup_phylonet_data(basedir: str,
                      config: BioConfig,
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    #Get the raxml's output and create a NEXUS file as output in the basedir
    phylonet_phase1 = config.phylonet_phase1
    gene_trees = f"{basedir}/{config.raxml_dir}/{config.raxml_output}"
    out_dir = f"{basedir}/phylonet_phase_1.nex"
    return f"{phylonet_phase1} -i {gene_trees} -o {out_dir} -r {config.phylonet_threads} -t {config.phylonet_threads} -hm {config.phylonet_hmax}"
    
@parsl.bash_app(executors=['snaq'])
def phylonet(basedir: str,
         config: BioConfig,
         inputs=[],
         outputs=[],
         stderr=parsl.AUTO_LOGNAME,
         stdout=parsl.AUTO_LOGNAME):

    exec_phylonet = config.phylonet
    input_file = f"{basedir}/phylonet_phase_1.nex"

    # Return to Parsl to be executed on the workflow
    return f'{exec_phylonet} {input_file}'
