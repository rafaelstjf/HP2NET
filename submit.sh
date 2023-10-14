#!/bin/bash
#SBATCH --nodes=1           #Numero de Nós
#SBATCH --ntasks-per-node=24 #Numero de tarefas por Nó
#SBATCH -p cpu_small         #Fila (partition) a ser utilizada
#SBATCH -J Biocomp             #Nome job
#SBATCH --exclusive          #Utilização exclusiva dos nós durante a execução do job
#SBATCH --time=08:00:00
#SBATCH -e slurm-%j.err
#SBATCH -o slurm-%j.out




echo Loading modules
module load python/3.9.6
source $SCRATCH/meu_python/bin/activate
#acessa o diretório onde o script está localizado
cd /scratch/pcmrnbio2/rafael.terra/WF_parsl/biocomp_single
#executa o script
echo Starting Workflow Script
echo Workload file: $WF
echo Maximum workers: $MW
python3 parsl_workflow.py --wf $WF --cf 1_thread.ini --mw $MW

#sbatch --export=ALL,SCRATCH=/scratch/pcmrnbio2/rafael.terra,WF=100g6t_iqtree_snaq.config,MW=1 submit.sh 