import parsl

from parsl import bash_app

#
# Parsl Bash Applications
#
@bash_app
def raxml(dataset : str, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
	"""Runs the Raxml's perl scrip (RPS) on a directory (input)

	Parameters:
		dataset (str) : the dataset/directory name to be processed by the RPS.
		inputs (list) : a list with contextual definitions.

	Returns:
		returns an parsl's AppFuture

	TODO: Provide provenance.

	NB:
		Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
		named according to task id and saved under task_logs in the run directory.
	"""
	import os
	import workflowconfig as wc

	# Get the configuration environment. 
	config = wc.getconfig("raxmlconfig")
	scriptdir = config["raxmlscripts"]
	datadir = config["datadir"]
	numcores = config["numcores"]
	seqdir = config["seqdir"]
	raxmldir = config["raxmldir"]
	astraldir = config["astraldir"]

	# Get the datapath and go.
	datapath = f"{datadir}/{dataset}"
	os.chdir(datapath)

	# Build the invocation command.
	cmd = f"{scriptdir}/raxml.pl --numCores={numcores} --seqdir={seqdir} --raxmldir={raxmldir} --astraldir={astraldir}"

	# Return to Parsl to be executed on the workflow
	return cmd

#
# Parsil configuration logic
#
import parsl

from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname

parslconfig = Config(
    executors=[
        HighThroughputExecutor(
            label="MyLaptop",
            max_workers=2,
            provider=LocalProvider(
                channel=LocalChannel()
            ),
        )
    ],
)


#
# Workflow logic
#
import time

def main():
	"""Main Workflow Function

	Parameters:
		None

	Returns:
		None

	NB:
		Loop over a workload list and dispach on different nodes, then pool over the results.
	"""

	workload = ["baseline.gamma0.3_n30", "baseline.gamma0.3_n100", "baseline.gamma0.3_n300",
				"baseline.gamma0.3_n1000", "n6.gamma0.30.2_n300", "n15.gamma0.30.20.2_n300"]

	# Create the list holding the Future Apps and populate it.
	result = []
	for i in workload:
		result.append(raxml(i))

	# Pool over the Futures and check if they are done.
	finished = False;
	while not finished:
		finished = True
		for r in result:
			if not r.done():
				finished = False
		time.sleep(30)

	# Retrieve values (should be zero).
	for r in result:
		retval = r.result()
		print(f"{retval}")

if __name__ == "__main__":
	# Dispatch the Parsl machine and call the main...
	parsl.load()
	main()