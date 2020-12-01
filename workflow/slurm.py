import os
import sys
from subprocess import check_output

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

	