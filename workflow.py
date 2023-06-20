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
import math
from parsl.channels import LocalChannel
from parsl.launchers import SrunLauncher, SingleNodeLauncher
from parsl.addresses import address_by_interface, address_by_hostname
from parsl.executors import HighThroughputExecutor, WorkQueueExecutor
from parsl.providers import LocalProvider, SlurmProvider
from datetime import datetime
from bioconfig import BioConfig
from typing import Any
# PARSL CONFIGURATION
def workflow_config(config: BioConfig, max_workers = None) -> parsl.config.Config:
    """ Configures and loads Parsl's Workflow configuration

    Parameters:

    config: BioConfig    - Workflow configuration
    """
    name = config.workflow_name
    interval = 30
    monitor = config.workflow_monitor
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
    parsl.set_stream_logger(level=logging.ERROR)
    parsl.set_file_logger(f'{name}_script_{date_time}.output', level=logging.DEBUG)
    if max_workers is not None:
        curr_workers = max_workers
    else:
        curr_workers = config.workflow_core
    logging.info('Configuring Parsl Workflow Infrastructure')

    # Read where datasets are...
    env_str = config.environ

    logging.info(f'Task Environment {env_str}')
    
    if config.execution_provider == "Slurm":
        mon_hub = parsl.monitoring.monitoring.MonitoringHub(
        workflow_name=name,
        hub_address=address_by_hostname(),
        resource_monitoring_enabled=True,
        monitoring_debug=False,
        resource_monitoring_interval=interval,
        ) if monitor else None
        return parsl.config.Config(
            executors=[
                HighThroughputExecutor(
                    label=f'single_partition',
                    # Optional: The network interface on node 0 which compute nodes can communicate with.
                    address=address_by_hostname(),
                    cores_per_worker=1,
                    max_workers=curr_workers,
                    worker_debug=False,
                    provider=LocalProvider(
                        nodes_per_block=1,
                        channel=LocalChannel(script_dir = config.script_dir),
                        parallelism=1,
                        init_blocks=config.workflow_node,
                        max_blocks=config.workflow_node,
                        worker_init=env_str,
                        launcher=SrunLauncher(overrides=f'-c {config.workflow_core}')
                    ),
                ),
            ],
            monitoring=mon_hub,
            strategy=None,
        )
    else:
        mon_hub = parsl.monitoring.monitoring.MonitoringHub(
        workflow_name=name,
        hub_address="127.0.0.1",
        resource_monitoring_enabled=True,
        monitoring_debug=False,
        resource_monitoring_interval=interval,
        ) if monitor else None
        return parsl.config.Config(
        executors=[
            HighThroughputExecutor(
                label=f'single_partition',
                # Optional: The network interface on node 0 which compute nodes can communicate with.
                address="127.0.0.1",
                cores_per_worker=1,
                max_workers=curr_workers,
                worker_debug=False,
                provider=LocalProvider(
                    nodes_per_block=1,
                    channel=LocalChannel(script_dir = config.script_dir),
                    parallelism=1,
                    init_blocks=config.workflow_node,
                    max_blocks=config.workflow_node,
                    worker_init=env_str,
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

class CircularList:
    def __init__(self, slots: int) -> None:
        if not slots:
            raise ValueError
        self.list = [None for _ in range(slots)]
        self.index = 0
        self.max_index = len(self.list) - 1

    def next(self) -> Any:
        if self.index == self.max_index:
            self.index = 0
        else:
            self.index += 1
        return self.list[self.index]

    def current(self, value: Any) -> None:
        self.list[self.index] = value
        return
