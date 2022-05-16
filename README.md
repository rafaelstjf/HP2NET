# Workflow for construction of phylogenetic networks on High Performance Computing (HPC) environment

## Introduction

Phylogeny refers to the evolutionary history and relationship between biological lineages related by common descent. Reticulate evolution refers to the origination of lineages through the complete or partial merging of ancestor lineages. Networks may be used to represent lineage independence events in non-treelike phylogenetic processes.

The methodology for reconstructing networks is still in development. Here we explore two methods for reconstructing rooted explicit phylogenetic networks, PhyloNetworks and Phylonet, which employ computationally expensive and time consuming algorithms. The construction of phylogenetic networks follows a coordinated processing flow of data sets analyzed and processed by the coordinated execution of a set of different programs, packages, libraries or pipelines, called workflow activities. 

In view of the complexity in modeling network experiments, the present work introduces a workflow for phylogenetic network analyses coupled to be executed in High-Performance Computing (HPC) environments. The workflow aims to integrate well-established software, pipelines and scripts, implementing a challenging task since these tools do not consistently profit from the HPC environment, leading to an increase in the expected makespan and idle computing resources.

## Requirements

1. Python >= 3.8
   1. Biopython >= 1.75
   2. Pandas >= 1.3.2
   3. Parsl >= 1.0
3. Raxml >= 8.2.12
4. Astral  >= 5.7.1
5. SnaQ (PhyloNetworks) >= 0.13.0
6. MrBayes >= 3.2.7a
7. BUCKy >=  1.4.4
8. Quartet MaxCut >= 2.10
9. PhyloNet >= 3.8.2
10. Julia >= 1.4.1
11. IQTREE >= 2.0


## How to use

### Setting up the workflow

The workflow uses a file to get all the needed parameters. For default it loads the file *default.ini* in the config folder, but you can explicitly load other files using the argument ``--cf name_of_the_file``, *e.g.* ``--cf config/test.ini``.

* Edit *parl.env* with the environment variables and add your folder to ``PYTHONPATH``.
* Edit *work.config* with the directories of your phylogeny studies (the workflow receives as input a set of homologous gene alignments of species in the nexus format).
* Edit *default.ini* with the path for each of the needed softwares and the parameters of the execution provider.

#### Contents of the configuration file

* General settings

```ini
[GENERAL]
ExecutionProvider = SlurmProvider
ScriptDir 		= ./scripts
Environ			= config/parsl.env
Workload		= config/work.config
NetworkMethod   = MP
TreeMethod      = RAXML
BootStrap       = 1000
```

1. The workflow can be executed in a HPC environment using the Slurm resource manager using the parameter ``ExecutionProvider`` equals to ``SlurmProvider`` or locally with ``LocalProvider``. 
2. The path of the scripts folder is assigned  in ``ScriptDir``. It's recommended to use the absolute path to avoid errors.
3. The ``Environ`` parameter contains the path of the file used to set environment variables. More details can be seen below.
4. In ``Workload`` is the path of the experiments that will be performed.
5. ``NetworkMethod`` and ``TreeMethod`` are the default network and tree methods that will be used to perform the workloads' studies.
6. ``Bootstrap`` is the parameter used in all the software that use bootstrap (RAxML, IQTREE and ASTRAL)

* Workflow execution settings

```ini
[WORKFLOW]
Monitor			= False
PartitionFast	= sequana_cpu
PartCoreFast	= 48
PartNodeFast	= 1
PartitionThread = sequana_cpu
PartCoreThread	= 48
PartNodeThread	= 4
PartitionLong	= sequana_cpu_long
PartCoreLong	= 48
PartNodeLong	= 1
WalltimeFast	= 00:30:00
WalltimeThread	= 00:30:00
WalltimeLong	= 02:00:00
```

1. ``Monitor`` is a parameter to use parsl's monitor module in HPC environment. It can be *true* or *false*. If you want to use it, it's necessary to set it as *true* and manually change the address in ``workflow.py``
2. If you are using it in a HPC environment, the workflow is going to submit three different jobs, with three different behaviours. ``PartitionFast`` is used for single threaded tasks, ``PartitionThread`` is for multi threaded software like RAxML and ``PartitionLong``is for PhyloNetworks and PhyloNet.
   1. If you are using it locally, the ``PartCore`` values are the number of threads that each class of software is going to use. The ``PartNode`` is the number of workers for each partition (the fast partition doesn't account for this behavior. It uses *PartCoreFast*, because each worker has only one thread).
   2. ``Walltime`` parameters are used only in the HPC environment.

* RAxML settings

  ```ini
  [RAXML]
  RaxmlExecutable = raxmlHPC-PTHREADS
  RaxmlThreads 	= 6
  RaxmlEvolutionaryModel = GTRGAMMA --HKY85
  ```

* IQTREE settings

  ```ini
  [IQTREE]
  IqTreeExecutable = iqtree2
  IqTreeEvolutionaryModel = TIM2+I+G 
  IqTreeThreads = 6
  ```

* ASTRAL settings

  ```ini
  [ASTRAL]
  AstralExecDir 	= /scratch/pcmrnbio2/app/softwares/astral/5.7.1
  AstralJar 		= astral.5.7.1.jar
  ```

* PhyloNet settings

  ```ini
  [PHYLONET]
  PhyloNetExecDir 	= /scratch/pcmrnbio2/softwares/phylonet/
  PhyloNetJar 		= PhyloNet_3.8.2.jar
  PhyloNetThreads     = 6
  PhyloNetHMax        = 3
  PhyloNetRuns        = 5
  ```

* SNAQ settings

  ```ini
  [SNAQ]
  SnaqThreads		= 6
  SnaqHMax        = 3
  SnaqRuns        = 3
  ```

* Mr. Bayes settings

  ```ini
  [MRBAYES]
  MBExecutable	= mb
  MBParameters	= mcmcp ngen=100000 burninfrac=.25 samplefreq=50 printfreq=10000 diagnfreq=10000 nruns=2 nchains=2 temp=0.40 swapfreq=10
  ```

* Bucky settings

  ```ini
  [BUCKY]
  BuckyExecutable = bucky
  MbSumExecutable = mbsum
  ```

* Quartet MaxCut

  ```ini
  QUARTETMAXCUT]
  QmcExecDir       = /scratch/pcmrnbio2/app/softwares/quartet/
  QmcExecutable    = find-cut-Linux-64
  ```

#### Workload file

For default the workload file is ``work.config`` in the *config* folder. The file contains the absolute paths of the experiment's folders.

```
/scratch/pcmrnbio2/rafael.terra/WF_parsl/data/Denv_1
```

You can comment folders using the # character in the beginning of the path. *e. g.* ``#/scratch/pcmrnbio2/rafael.terra/WF_parsl/data/Denv_1``. That way the workflow won't read this path.

You can also run a specific flow for a path using ``@TreeMethod|NetworkMethod`` in the end of a path. Where *TreeMethod* can be RAXML, IQTREE or MRBAYES and *NetworkMethod* can be MPL or MP (case sensitive). The supported flows are: ``RAXML|MPL``, ``RAXML|MP``, ``IQTREE|MPL``, ``IQTREE|MP`` and ``MRBAYES|MPL``. For example:

```
/scratch/pcmrnbio2/rafael.terra/WF_parsl/data/Denv_1@RAXML|MPL
```

#### Environment file

The environment file contains all the environment variables (like module files used in Slurm) used during the workflow execution, including the Python Path. It's very important to set the workflow's absolute path in the Python Path present in this file. Example:

```sh
module load python/3.8.2
module load raxml/8.2_openmpi-2.0_gnu
module load java/jdk-12
module load iqtree/2.1.1
module load bucky/1.4.4
module load mrbayes/3.2.7a-OpenMPI-4.0.4
source /scratch/app/modulos/julia-1.5.1.sh
export PYTHONPATH=$PYTHONPATH:/scratch/pcmrnbio2/rafael.terra/WF_parsl/biocomp
```

#### Experiment folder

Each experiment folder needs to have a *input folder* containing a *.tar.gz* compressed file and a *.json* with the following content. **The workflow considers that there is only one file of each extension in the input folder**.

```json
{
	"Mapping":"",
	"Outgroup":""
}
```

Where ``Mapping`` is a direct mapping of the taxon, when there are multiple alleles per species, in the format ``species1:taxon1,taxon2; species2: taxon3, taxon4``and ``Outgroup`` is the taxon used to root the network. The Mapping parameter is optional (although it has to be in the json file without value), but the outgroup is obligatory. It's important to say that the flow *MRBAYES|MPL* doesn't support multiple alleles per species.

## Running the workflow

After setting up the workflow, just run ``python3 parsl_workflow.py`` in a login machine. Parsl will be responsible to submit the job of the tasks in the HPC environment.

The workflow is under heavy development. If you notice any bug, please create an issue here on GitHub.

---
## If you use it, please cite

Terra, R., Coelho, M., Cruz, L., Garcia-Zapata, M., Gadelha, L., Osthoff, C., ... & Ocana, K. (2021, July). Gerência e Análises de Workflows aplicados a Redes Filogenéticas de Genomas de Dengue no Brasil. In *Anais do XV Brazilian e-Science Workshop* (pp. 49-56). SBC.

**Also cite all the coupled software!**

