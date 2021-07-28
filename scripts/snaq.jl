using Distributed: length
#!/bin/env julia

#use the args to generate the correct output
#arg[1] = 0 if phylogenetic tree, 1 if table of concordance factors
#arg[2]= path of the tree
#arg[3] = path of the topology
#arg[4] = output dir
#arg[5] = num_workers
#arg[6] = hmax

println("Starting PhyloNetworks...")
if length(ARGS) < 6
    println("Missing arguments!")
else
    println("Tree method: ", ARGS[1])
    println("Path of the tree: ", ARGS[2])
    println("Path of the topology: ", ARGS[3])
    println("Output folder: ", ARGS[4])
    println("Number of processors: ", ARGS[5])
    println("Hybridization max: ", ARGS[6])
end
using PhyloNetworks
using PhyloPlots
using Distributed

addprocs(parse(Int64,ARGS[5]) - 1)

#numproc = Threads.nthreads()
#println("Number of Threads: ", numproc)
basedir = dirname(ARGS[4])
output = joinpath(ARGS[4], replace(basename(basedir),"/" => "" ))
println("Using PhyloNetworks on every processor")
@everywhere using PhyloNetworks
@everywhere using PhyloPlots
if parse(Int64,ARGS[1]) == 0
    raxmlCF = readTrees2CF(ARGS[2], writeTab=false, writeSummary=false)
    astraltree = last(readMultiTopology(ARGS[3])) # main tree with BS as node labels
    net = snaq!(astraltree,  raxmlCF, hmax=parse(Int64,ARGS[6]), filename=string(output))
elseif parse(Int64,ARGS[1]) == 1
    buckyCF = readTableCF(ARGS[2])
    tre = readTopology(ARGS[3])
    net = snaq!(tre,  buckyCF, hmax=parse(Int64,ARGS[6]), filename=string(output))
else
    println("Wrong argument!")
    exit(1)
end