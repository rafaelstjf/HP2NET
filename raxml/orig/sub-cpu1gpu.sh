#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1    #Numero de tarefas por Nó
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24               #Numero de threads por tarefa
#SBATCH -p nvidia_small                       #Fila (partition) a ser utilizada
#SBATCH -J Beast1.10
#SBATCH --exclusive                     #Utilização exclusiva dos nós durante a execução do job


echo $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST

cd $SLURM_SUBMIT_DIR

module load beagle/current
module load beast/1.10
module load java/jdk-8u201

export JAVA_HOME=/scratch/app/java/jdk1.8.0_201
export PATH=${JAVA_HOME}/bin:${PATH}
export CLASSPATH=${JAVA_HOME}/lib/tools.jar:.

ulimit -s unlimited
ulimit -c unlimited
ulimit -v unlimited

##Diretório onde irá colocar a saída do slurm
cd /scratch/cenapadrjsd/micaella.paula/beast/result 

#### XML de entrada ####
input_file=/scratch/pcmrnbio2/guilherme.dornelas2/exe/beast/data/100mil


##### Executar esse para CPU 1GPU ######
    EXEC="java -Xms64m -Xmx24G -jar /scratch/app/beast/1.10/lib/beast.jar -overwrite -beagle -beagle_SSE -beagle_CPU -threads $SLURM_CPUS_PER_TASK  -beagle_GPU -beagle_cuda -beagle_order 0,1,0,1,0,1,0,1,0,1 ${input_file}"

time srun --resv-ports -n 1 -c $SLURM_CPUS_PER_TASK $EXEC

