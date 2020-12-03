import os
import os.path as path
import sys
from workflow import slurm

def main():

	homedir = os.getcwd()
	os.system('rm -fr workdir')
	workdir = homedir + '/' + 'workdir'
	os.mkdir(workdir)

	# Create a list of sequences to be submited...
	sequences_list = list()
	with open("work.config") as file:
		for line in file:
			line = line.strip()
			sequences_list.append(line)

	# Create all input sequences (nexus format)...
	for seq in sequences_list:
		print(f'Working on {seq}...')
		os.chdir(seq)
		os.chdir('input')
		os.system('rm -fr nexus')
		os.mkdir('nexus')
		os.chdir('nexus')
		os.system('tar zxf ../1_seqgen.tar.gz')
		os.chdir('../..')
		baseseq = path.basename(seq)
		os.system('perl /scratch/cenapadrjsd/diego.carvalho/bio/biocomp/workflow/scripts/raxml-phase1.pl --seqdir=input/nexus --raxmldir=raxml --astraldir=astral')
		pwd = os.getcwd()
		print(f'Moving to {workdir}/{baseseq}.cmd - on {pwd}')
		os.chdir('raxml')
		os.system('rm -f rm RAxML_*')
		os.rename('raxml.cmd',f'{workdir}/{baseseq}.cmd')
		os.chdir(homedir)

	os.chdir(workdir)
	
	return

if __name__ == "__main__":
	main()