using Distributed: length
#!/bin/env julia

#use the args to generate the correct output
#arg[1] = 0 if phylogenetic tree, 1 if table of concordance factors
#arg[2]= path of the tree
#arg[3] = path of the topology
#arg[4] = output dir
#arg[5] = num_cpu
#arg[6] = hmax

println("Starting PhyloNetworks...")
for arg in ARGS
    println(arg)
end
if length(ARGS) < 6
    println("Missing arguments!")
end
using PhyloNetworks
using PhyloPlots
using Distributed

addprocs(parse(Int64,ARGS[5]) - 1)

numproc = Threads.nthreads()

println("Number of Threads: ", numproc)

println("Issuing using PhyloNetworks on every thread")
@everywhere using PhyloNetworks
@everywhere using PhyloPlots
if parse(Int64,ARGS[1]) == 0
    raxmlCF = readTrees2CF(ARGS[2], writeTab=false, writeSummary=false)
    astraltree = last(readMultiTopology(ARGS[3])) # main tree with BS as node labels
    net = snaq!(astraltree,  raxmlCF, hmax=ARGS[6], filename=string(ARGS[4], "phylogenetic_network"))
elseif parse(Int64,ARGS[1]) == 1
    buckyCF = readTableCF(ARGS[3])
    tre = readTopology(ARGS[4])
    net = snaq!(tre,  buckyCF, hmax=ARGS[6], filename=string(ARGS[4], "phylogenetic_network"))
else
    println("Wrong argument!")
    exit(1)