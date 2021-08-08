# -*- coding: utf-8 -*-

""" Workflow.py. Parsl Configuration Functions (@) 2021

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


import parsl
import logging
from parsl.channels import LocalChannel
from parsl.launchers import SrunLauncher, SingleNodeLauncher
from parsl.addresses import address_by_interface
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider, SlurmProvider

from bioconfig import BioConfig

# PARSL CONFIGURATION


def workflow_config(config: BioConfig, ) -> parsl.config.Config:
    """ Configures and loads Parsl's Workflow configuration

    Parameters:

    config: BioConfig    - Workflow configuration
    """
    name = config.workflow_name
    interval = 30
    monitor = config.workflow_monitor

    parsl.set_stream_logger(level=logging.ERROR)
    parsl.set_file_logger(f'{name}_script.output', level=logging.DEBUG)

    logging.info('Configuring Parsl Workflow Infrastructure')

    # Read where datasets are...
    env_str = config.environ

    logging.info(f'Task Environment {env_str}')
    mon_hub = parsl.monitoring.monitoring.MonitoringHub(
            workflow_name=name,
            hub_address=address_by_interface('ib0'),
            hub_port=60001,
            resource_monitoring_enabled=True,
            monitoring_debug=False,
            resource_monitoring_interval=interval,
        ) if monitor else None
    if config.execution_provider == 'SlurmProvider':
        return parsl.config.Config(
            executors=[
                HighThroughputExecutor(
                    label='single_thread',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    address=address_by_interface('ib0'),
                    max_workers=int(config.workflow_core_f),
                    cores_per_worker=1,
                    worker_debug=False,
                    interchange_port_range=(50000, 55000),
                    provider=SlurmProvider(
                        partition=config.workflow_part_f,
                        # scheduler_options='',
                        parallelism=1,
                        init_blocks=1,
                        max_blocks=1,
                        nodes_per_block=config.workflow_node_f,
                        cmd_timeout=120,
                        worker_init=env_str,
                        move_files=False,
                        walltime=config.workflow_wall_t_f,
                        launcher=SrunLauncher(
                            overrides=f'-c {config.workflow_core_f}'),
                    ),
                ),
                HighThroughputExecutor(
                    label=f'tree_and_statistics',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    address=address_by_interface('ib0'),
                    max_workers=int(config.workflow_core_t),
                    cores_per_worker=6,
                    worker_debug=False,
                    interchange_port_range=(55000, 60000),
                    provider=SlurmProvider(
                        partition=config.workflow_part_t,
                        # scheduler_options='',
                        parallelism=1,
                        init_blocks=1,
                        max_blocks=1,
                        nodes_per_block=config.workflow_node_t,
                        cmd_timeout=120,
                        worker_init=env_str,
                        move_files=False,
                        walltime=config.workflow_wall_t_t,
                        launcher=SrunLauncher(
                            overrides=f'-c {config.workflow_core_t}'),
                    ),
                ),
                HighThroughputExecutor(
                    label=f'phylogenetic_network',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    address=address_by_interface('ib0'),
                    max_workers=int(config.workflow_core_l),
                    cores_per_worker=6,
                    worker_debug=False,
                    interchange_port_range=(40000, 45000),
                    provider=SlurmProvider(
                        partition=config.workflow_part_l,
                        # scheduler_options='',
                        parallelism=1,
                        init_blocks=1,
                        max_blocks=1,
                        nodes_per_block=config.workflow_node_l,
                        cmd_timeout=120,
                        worker_init=env_str,
                        move_files=False,
                        walltime=config.workflow_wall_t_l,
                        launcher=SrunLauncher(
                            overrides=f'-c {config.workflow_core_l}'),
                    ),
                ),

            ],
            monitoring=mon_hub,
            strategy=None,
        )
    else: #localprovider
        return parsl.config.Config(
            executors=[
                HighThroughputExecutor(
                    label='single_thread',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    max_workers=3,
                    cores_per_worker=1,
                    worker_debug=False,
                    provider=LocalProvider(
                        channel=LocalChannel(script_dir="/mnt/c/Users/rafae/Documents/GitHub/biocomp"),
                        parallelism=1,
                        init_blocks=1,
                        worker_init=env_str,
                        max_blocks=1
                    ),
                ),
                HighThroughputExecutor(
                    label=f'tree_and_statistics',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    max_workers=1,
                    cores_per_worker=4,
                    worker_debug=False,
                    provider=LocalProvider(
                        channel=LocalChannel(script_dir="/mnt/c/Users/rafae/Documents/GitHub/biocomp"),
                        parallelism=1,
                        init_blocks=1,
                        worker_init=env_str,
                        max_blocks=1
                    ),
                ),
                HighThroughputExecutor(
                    label=f'phylogenetic_network',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    # address=address_by_interface('enp4s0f0' or 'ib0')
                    max_workers=1,
                    cores_per_worker=4,
                    worker_debug=False,
                    provider=LocalProvider(
                        channel=LocalChannel(script_dir="/mnt/c/Users/rafae/Documents/GitHub/biocomp"),
                        parallelism=1,
                        init_blocks=1,
                        worker_init=env_str,
                        max_blocks=1
                    ),
                ),
            ],
            monitoring=mon_hub,
            strategy=None,
        )

# SYNCHRONIZATION ROUTINES


def wait_for_all(list_of_futures: list, sleep_interval=10) -> None:
    """ Wait for parsl's future completion.

        Loops over the list of futures and check if everone were done.
        If not, sleep for sleep_interval.
    Parameters:
        list_of_futures (list): a list of parsl's futures.
        sleep_interval (int): sleep interval
    """
    import time

    # TODO: must find a better algorithm, since there are different
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
