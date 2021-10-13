using Distributed: length
#!/bin/env julia

#use the args to generate the correct output
#arg[1] = tree method
#arg[2]= path of the tree
#arg[3] = path of the topology
#arg[4] = output dir
#arg[5] = num_workers
#arg[6] = hmax
#arg[7] = runs
#arg[8] = outgroup

println("Starting PhyloNetworks...")
if length(ARGS) < 8
    println("Missing arguments!")
else
    println("Tree method: ", ARGS[1])
    println("Path of the tree: ", ARGS[2])
    println("Path of the topology: ", ARGS[3])
    println("Output folder: ", ARGS[4])
    println("Number of processors: ", ARGS[5])
    println("Hybridization max: ", ARGS[6])
    println("Number of runs max: ", ARGS[7])
    println("Outgroup taxon: ", ARGS[8])
    if(length(ARGS) == 9)
        println("Species mapping: ", ARGS[9])
    end
end
using PhyloNetworks
using PhyloPlots
using Distributed

addprocs(parse(Int64,ARGS[5]) - 1)

basedir = dirname(ARGS[4])
name = string(replace(basename(basedir),"/" => "" ), "_", ARGS[1], "_", "MPL_", ARGS[6])
output = joinpath(ARGS[4], name)
println("Using PhyloNetworks on every processor")
@everywhere using PhyloNetworks
@everywhere using PhyloPlots
if ARGS[1] == "RAXML" || ARGS[1] == "IQTREE"
    if length(ARGS) == 9
        genetrees = readMultiTopology(ARGS[2])
        taxon_map = Dict{String, String}()
        spec_list = split(ARGS[9], ';')
        for spec in spec_list
            sp = split(strip(spec), ':')
            for allele in split(sp[2], ',')
                merge!(taxon_map, Dict(allele=>sp[1]))
            end
        end
        df_sp = writeTableCF(countquartetsintrees(genetrees, taxonmap))
        CSV.write(joinpath(ARGS[4], "tableCF_species.csv"), df_sp)
        raxmlCF = readTableCF(joinpath(ARGS[4], "tableCF_species.csv"))
    else
        raxmlCF = readTrees2CF(ARGS[2], writeTab=false, writeSummary=false)
        astraltree = readTopology(last(readlines(ARGS[3]))) # main tree with BS as node labels
        net = snaq!(astraltree,  raxmlCF, hmax=parse(Int64,ARGS[6]), filename=string(output), runs=parse(Int64,ARGS[7]), outgroup=ARGS[8])
    end

elseif ARGS[1] == "MRBAYES"
    buckyCF = readTableCF(ARGS[2])
    qmc_tree = readTopology(ARGS[3])
    net = snaq!(qmc_tree,  buckyCF, hmax=parse(Int64,ARGS[6]), filename=string(output), runs=parse(Int64,ARGS[7]), outgroup=ARGS[8])
else
    println("Wrong argument!")
    exit(1)
end