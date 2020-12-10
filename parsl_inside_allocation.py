import logging
from os import curdir

### LOGGING SECTION
logging.basicConfig(level=logging.INFO)

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

    parsl.load(config)
    return


def wait_for_all(list_of_futures, sleep=60):
    import time

    ready_list = [1 for _ in list_of_futures]

    while sum(ready_list) > 0:
        # TODO: checks everytime all items, including any already done.
        # Must check if we can copy Parsl.Future objects.
        for id, r in enumerate(list_of_futures):
            if r.done():
                ready_list[id] = 0
            time.sleep(sleep)

#
# Parsl Bash Applications
#
import parsl
from parsl import bash_app
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
    import random

    # Build the invocation command.
    input_file = datadir
    output_file = 1#outputs[0]

    cmd = f"java astral {flags} -p -s {input_file} -n {output_file}"
    cmd = f"echo {cmd}" 
    # Return to Parsl to be executed on the workflow
    return cmd

def loop_on_baseline_raxml(basedir, datalist):
    result = list()
    for input_file in datalist:
        fut_result = raxml(basedir, inputs=[input_file])
        result.append(fut_result)

    return result
        

if __name__ == "__main__":
    import os
    import glob
    logging.info('Starting the Workflow Orchastration') 
    
    #Configure the infrastructure
    #TODO: Fetch the configuration from a file...
    workflow_config(name='BioWorkFlow',
                    nodes=2,
                    cores_per_node=8,
                    interval=1,
                    monitor=False)

    #Read where datasets are...
    work_list = list()
    with open('work.config', 'r') as reader:
        for line in reader.readlines():
            baseline = line.strip()
            work_list.append(baseline)
            logging.info(f'Adding {baseline} to the list')


    result = list()
    for basedir in work_list:
        #TODO: Convert nexus to phylip
        base_file_list = glob.glob(basedir + '/input/phylip/*')
        rf = loop_on_baseline_raxml(basedir,base_file_list)
        for i in rf:
            result.append(i)

    logging.info(f'Entering in wait_for_all raxml')
    wait_for_all(result)

    logging.info(f'Running Astral')
    result = list()
    for basedir in work_list:
        raxml_dir = f'{basedir}/raxml'
        os.system(f'rm {raxml_dir}/RAxML_info.*')
        try:
            os.mkdir(f'{raxml_dir}/bootstrap')
        except FileExistsError:
            os.system(f'rm -f {raxml_dir}/bootstrap/*')
        os.system(f'mv {raxml_dir}/RAxML_bootstrap.* {raxml_dir}/bootstrap')
        os.system(f'tar -czf {raxml_dir}/contrees.tgz {raxml_dir}/RAxML_bipartitions*')
        os.system(f'rm -f {raxml_dir}/RAxML_bipartitions*')
        os.system(f'cat {raxml_dir}/RAxML_bestTree.* > {raxml_dir}/besttrees.tre')
        os.system(f'tar -czf {raxml_dir}/besttrees.tgz {raxml_dir}/RAxML_bestTree.*')
        os.system(f'rm -f {raxml_dir}/RAxML_bestTree.*')
        r = astral(basedir)
        result.append(r)
        
    logging.info(f'Entering in wait_for_all atral')
    wait_for_all(result)
