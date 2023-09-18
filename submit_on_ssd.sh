#!/bin/bash
#SBATCH --nodes=1           #Numero de Nós
#SBATCH --ntasks-per-node=24 #Numero de tarefas por Nó
#SBATCH -p cpu_small         #Fila (partition) a ser utilizada
#SBATCH -J HP2NET             #Nome job
#SBATCH --exclusive          #Utilização exclusiva dos nós durante a execução do job
#SBATCH --time=07:00:00
#SBATCH -e slurm-%j.err
#SBATCH -o slurm-%j.out

#informacoes
#lsblk
#echo "of no ssd"
#dd if=/dev/zero of=/tmp/out.txt oflag=direct bs=128k count=16k
#echo "of no scratch"
#dd if=/dev/zero of=/scratch/pcmrnbio2/rafael.terra/out.txt oflag=direct bs=128k count=16k

SSD_DIR="/tmp/rafael"
#cria pasta no ssd
mkdir -p $SSD_DIR
#lê o arquivo de workload 
new_wf="$WF.ssd"
buffer=''
newline=$'\n'
while IFS= read -r linha || [[ -n "$linha" ]]; do
    echo "$linha"
    split=($(echo "$linha" | tr '@' ' '))
    split_slash=($(echo "${split[0]}" | tr '/' ' '))
    folder_name=${split_slash[-1]}
    new_folder="${SSD_DIR}/${folder_name}"
    mkdir -p $new_folder
    mkdir -p "${new_folder}/input"
    cp -R "${split[0]}/input/." "${new_folder}/input"
    buffer="${buffer}${new_folder}@${split[-1]}"
    buffer+="${newline}"
done < "$WF"
    echo "$buffer" > $new_wf


echo Loading modules
module load python/3.8.2
#acessa o diretório onde o script está localizado
cd /scratch/pcmrnbio2/rafael.terra/WF_parsl/biocomp_single_ssd
#executa o script
echo Starting Workflow Script
python3 parsl_workflow.py --wf $new_wf --cf teste.ini
