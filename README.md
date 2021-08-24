# Workflow for construction of phylogenetic networks on High Performance Computing environment

## Introduction

Phylogeny refers to the evolutionary history and relationship between biological lineages related by common descent. In phylogenetics, recombination may be revealed through incongruence of trees in regions of genomes with occurrence of recombination. 

Reticulate evolution refers to the origination of lineages through the complete or partial merging of ancestor lineages. Networks may be used to represent lineage independence events in non-treelike phylogenetic processes. In terms of information through reticulate evolution, genetic recombination entails the splitting and rejoining of two unrelated or distantly related DNA to form a new DNA. As the recombinant sequence is a merger of two evolutionary histories, such a process cannot be modeled by trees. Instead, a phylogenetic network representation of a recombination event is needed. 

The methodology for reconstructing networks is still in development. Here we explore two methods for reconstructing unrooted phylogenetic networks, PhyloNetworks and Phylonet, which employ computationally expensive and time consuming algorithms. The construction of phylogenetic networks follows a coordinated processing flow of data sets analyzed and processed by the coordinated execution of a set of different programs, packages, libraries or pipelines, called workflow activities. 

In view of the complexity in modeling network experiments, the present work introduces a workflow for phylogenetic network analyses coupled to be executed in High-Performance Computing (HPC) environments. The workflow aims to integrate well-established software, pipelines and scripts, implementing a challenging task since these tools do not consistently profit from the HPC environment, leading to an increase in the expected makespan and idle computing resources. At first, we draw a straightforward workflow without optimization to create phylogenetic networks aiming to observe the performance of the employed tools. Parsl -a scalable parallel

## Requirements

1. Python >= 3.8
   1. Biopython >= 1.75
   2. Pandas
   3. Parsl >= 1.0
3. Raxml >= 8.2.12
4. Astral  >= 5.7.1
5. SnaQ (PhyloNetworks) >= 0.13.0
6. MrBayes >= 3.2.7a
7. BUCKy >=  1.4.4
8. Quartet MaxCut >= 2.10
9. PhyloNet >= 3.8.2
10. Julia >= 1.5

## How to use

#### Setting up the workflow

* Edit *parl.env* with the environment.
* Edit *work.config* with the directories of your phylogeny studies (the workflow receives as input a set of homologous gene alignments of species in the nexus format).
* Edit *default.ini* with the path for each of the needed softwares and the parameters of the execution provider.

#### Running the workflow

After setting up the workflow, just run ``python parsl_workflow.py`` in a login machine. Parsl will be responsible to submit the job of the tasks

---
## If you use it, please cite

Terra, R., Coelho, M., Cruz, L., Garcia-Zapata, M., Gadelha, L., Osthoff, C., ... & Ocana, K. (2021, July). Gerência e Análises de Workflows aplicados a Redes Filogenéticas de Genomas de Dengue no Brasil. In *Anais do XV Brazilian e-Science Workshop* (pp. 49-56). SBC.