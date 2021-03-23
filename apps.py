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


# setup_phylip_data bash app


@parsl.bash_app(executors=['single_thread'])
def setup_phylip_data(datadir: str,
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Convert the nexus format to phylip in order to run Raxml on a directory.

    Parameters:
        datadir: str --- Directory where raxml-phase1 is going to search for a tar file with 
                         nexus files. The script will create:
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
    import bioconfig
    from appsexception import PhylipMissingData

    c = bioconfig.BioConfig()

    input_dir = datadir + '/input'
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
    return f'cd {datadir}; {c.raxml_phase1}'


# raxml bash app
@parsl.bash_app(executors=['raxml'])
def raxml(datadir: str,
          input_file: str,
          flags=None,
          num_threads=6, inputs=[],
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
    import bioconfig

    c = bioconfig.BioConfig()

    logging.info(f'raxml called with {datadir}')
    raxml_dir = datadir + '/raxml'

    # TODO: Create the following parameters(external configuration): -m, -N,
    if not flags:
        flags = f"-T {num_threads} -m GTRGAMMA --HKY85 -f a -N 100"

    p = random.randint(1, 10000)
    x = random.randint(1, 10000)

    output_file = os.path.basename(input_file).split('.')[0]

    # Return to Parsl to be executed on the workflow
    return f"cd {raxml_dir}; {c.raxml} {flags} -p {p} -x {x} -s {input_file} -n {output_file}"


@parsl.bash_app(executors=['single_thread'])
def setup_astral_data(datadir: str, inputs=[], outputs=[], flags=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    from bioconfig import BioConfig

    c = BioConfig()

    return f'{c.astral_phase1} {c.raxml_dir(datadir)}'


@parsl.bash_app(executors=['single_thread'])
def astral(datadir: str, inputs=[], outputs=[], flags=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
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
    import bioconfig

    c = bioconfig.BioConfig()

    # Build the invocation command.

    # TODO: manage the fixed jar pointer...
    bs_file = f'{c.astral_dir(datadir)}/BSlistfiles'
    # TODO: manage the fixed bootstrap number...
    num_boot = 100

    # Create bs_file
    with open(bs_file, 'w') as f:
        for i in glob.glob(f'{datadir}/raxml/bootstrap/*'):
            f.write(f'{i}\n')

    # Return to Parsl to be executed on the workflow
    return f'{c.astral} -i {c.raxml_output(datadir)} -b {bs_file} -r {num_boot} -o {c.astral_output(datadir)}'


# TODO: Export the parameter hmax (in .jl)
@parsl.bash_app(executors=['snaq', 'snaq_l'])
def snaq(datadir: str, num_threads=6, inputs=[], outputs=[], flags=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    import bioconfig

    c = bioconfig.BioConfig()

    return f'{c.snaq} {datadir} {num_threads}'
