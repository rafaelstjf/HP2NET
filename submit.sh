#!/bin/bash
#SBATCH --nodes=4           #Numero de Nós
#SBATCH --ntasks-per-node=48 #Numero de tarefas por Nó
#SBATCH -p sequana_cpu_long         #Fila (partition) a ser utilizada
#SBATCH -J Biocomp             #Nome job
#SBATCH --exclusive          #Utilização exclusiva dos nós durante a execução do job
#SBATCH --time=744:00:00
#SBATCH -e slurm-%j.err
#SBATCH -o slurm-%j.out



#Exibe os nos alocados para o Job
echo $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST

echo -n Entering in: 
pwd
cd $SLURM_SUBMIT_DIR

echo $SLURM_SUBMIT_HOST >> $NETINFO
ip addr >> $NETINFO

echo Loading modules
#Language, applications, and other configurations
module load python/3.8.2
#acessa o diretório onde o script está localizado
cd /scratch/pcmrnbio2/rafael.terra/WF_parsl/biocomp
mkdir -p log tmp
NETINFO=log/netinfo.$SLURM_JOBID.log
#executa o script
echo Starting Workflow Script
python3 parsl_workflow.py