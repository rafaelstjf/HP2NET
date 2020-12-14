import os
import sys
from subprocess import check_output

def create(jobfile, jobname, execpart, nodes=1, taskspernode=1, totaltasks=1, partition="cpu_dev", modules=["python/3.8.2"]):
	""" Create a SLURM job description on jobfile
	Parameters:
		jobfile (str): path to jobfile
		jobname (str): job name
		execpart (str): commands that will be executed by the job
		nodes (int): number of nodes
		taskspernode (int): number of cores allocated per node
		totaltasks (int): total number of tasks on the job
		partition (str): submition partion or queue
		modules (list): list of tags representing modules to be loaded/added
	Returns:
		None
	"""
	preamble = f"""
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={taskspernode}
#SBATCH --ntasks={totaltasks}          
#SBATCH -p {partition}          
#SBATCH -J {jobname}           
#SBATCH --exclusive

echo BIOCOMP Workflow Engine
echo Job created by slurm.py

#
echo SLURM JobID: $SLURM_JOB_ID
echo Allocated nodes: $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST

#
cd $SLURM_SUBMIT_DIR

"""

	jobmodules = str()
	for i in modules:
		jobmodules += f"module load {i}\n"

	template = preamble + jobmodules + execpart

	with open(jobfile,"w") as f:
		f.write(template)

	return

def submit(jobfile, dep=None):
	""" Submit a SLURM job pointed by jobfile
	Parameters:
		jobfile (str): path to jobfile
		dep (str): a string with a job id for dependency slurm (sbatch) argments
	Returns:
		jobid (str): a string to jobid
	NB:
		args is meant to link jobs as args = "--dependency=after:993", where 993 is a jobid. 
	"""
	if dep is None:
		cmd = f"sbatch {jobfile}"
	else:
		cmd = f"sbatch --dependency=after:{dep} {jobfile}"

	output = check_output(cmd)
	output = output.split()

	return output[3]

	