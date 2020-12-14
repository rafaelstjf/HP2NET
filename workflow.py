import logging

### PARSL CONFIGURATION
def workflow_config(name, nodes, cores_per_node=24, interval=30, monitor=False):
    import parsl
    from parsl.config import Config
    from parsl.channels import LocalChannel
    from parsl.launchers import SrunLauncher
    from parsl.providers import LocalProvider
    from parsl.addresses import address_by_interface
    from parsl.executors import HighThroughputExecutor
    from parsl.monitoring.monitoring import MonitoringHub

    parsl.set_stream_logger()
    parsl.set_file_logger('script.output', level=logging.DEBUG)

    logging.info('Configuring Parsl Workflow Infrastructure')

    #Read where datasets are...
    env_str = str()
    with open('parsl.env', 'r') as reader:
        env_str = reader.read()
    
    logging.info(f'Task Environment {env_str}')
    
    mon_hub = MonitoringHub(
            workflow_name=name,
            hub_address=address_by_interface('ib0'),
            hub_port=60001,
            resource_monitoring_enabled=True,
            monitoring_debug=False,
            resource_monitoring_interval=interval,
        ) if monitor else None

    config = Config(
        executors=[
            HighThroughputExecutor(
                label=name,
                # Optional: The network interface on node 0 which compute nodes can communicate with.
                # address=address_by_interface('enp4s0f0' or 'ib0')
                address=address_by_interface('ib0'),
                # one worker per manager / node
                max_workers=cores_per_node,
                provider=LocalProvider(
                    channel=LocalChannel(script_dir='.'),
                    # make sure the nodes_per_block matches the nodes requested in the submit script in the next step
                    nodes_per_block=nodes,
                    # make sure 
                    launcher=SrunLauncher(overrides=f'-c {cores_per_node}'),
                    cmd_timeout=120,
                    init_blocks=1,
                    max_blocks=1,
                    worker_init=env_str,
                ),
            )
        ],
        monitoring=mon_hub,
        strategy=None,
    )

    logging.info('Loading Parsl Config')

    parsl.load(config)
    return


### SYNCHRONIZATION ROUTINE
def wait_for_all(list_of_futures, sleep_interval=10):
    """ Wait for parsl's future completion.

        Loops over the list of futures and check if everone were done. 
        If not, sleep for sleep_interval.
    Parameters:
        list_of_futures (list): a list of parsl's futures.
        sleep_interval (int): sleep interval
    """
    import time

    #TODO: must find a better algorithm, since there are different
    # workflows being executed on parallel (several DAGs).
    # Perhaps, DAG executer should be implemented, where the user
    # may provide several workflows (DAGs) and the may be enacted by
    # a scheduler.
    
    # Loop
    not_done = True
    while not_done:
        not_done = False
        for r in list_of_futures:
            if not r.done():
                not_done = True
                break
        time.sleep(sleep_interval)

    # Fetch status (just inform parsl that we can proceed)
    for r in list_of_futures:
        r.result()

    return

#
# Parsl Bash and Python Applications
#
import parsl
from parsl import bash_app, python_app

# setup_phylip_data bash app
@bash_app
def setup_phylip_data(datadir: str, inputs=[], outputs=[], flags=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    """Open Runs the Raxml's executable (RPS) on a directory (input)

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
    import glob
    
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
                print('TAR ERROR!!! TODO:')
            # So, loop over and untar every file
            for tar_file in tar_file_list:
                os.system(f'cd {input_nexus_dir}; tar zxvf {tar_file}')
    # Now, use the modified script to convert nexus to phylip.
    return f'cd {datadir}; perl /scratch/cenapadrjsd/diego.carvalho/biocomp/raxml-phase1.pl  --seqdir=input/nexus --raxmldir=raxml --astraldir=astral'

# raxml bash app
@bash_app
def raxml(datadir: str, inputs=[], outputs=[], flags=False, stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
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
    import logging
    import random
    import glob
    import os

    logging.info(f'raxml called with {datadir}')
    raxml_dir = datadir + '/raxml'

    if not flags:
        flags = "-T 6 -m GTRGAMMA --HKY85 -f a -N 100"

    p = random.randint(0, 10000)
    x = random.randint(0, 10000)

    input_file = inputs[0]
    output_file = os.path.basename(input_file).split('.')[0]

    # Build the invocation command.
    cmd = f"cd {raxml_dir}; module load raxml/8.2_openmpi-2.0_gnu; raxmlHPC-PTHREADS {flags} -p {p} -x {x} -s {input_file} -n {output_file}"
    # Return to Parsl to be executed on the workflow
    return cmd


@bash_app
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
    import os
    import glob
    import random

    # Build the invocation command.

    #TODO: manage the fixed jar pointer...
    astral_exec_dir ='/scratch/cenapadrjsd/diego.carvalho/biocomp/Astral/'
    astral_jar = 'astral.5.7.4.jar'
    astral_dir = f'{datadir}/astral'
    input_file = f'{datadir}/raxml/besttrees.tre'
    bs_file = f'{astral_dir}/BSlistfiles'
    #TODO: manage the fixed bootstrap number...
    num_boot = 100
    astral_out = f'{astral_dir}/astral.tre'

    #Create bs_file
    with open(bs_file,'w') as f:
        for i in glob.glob(f'{datadir}/raxml/bootstrap/*'):
            f.write(f'{i}\n')

    cmd = f'cd {astral_exec_dir}; java -jar {astral_jar} -i {input_file} -b {bs_file} -r {num_boot} -o {astral_out}'

    # Return to Parsl to be executed on the workflow
    return cmd

