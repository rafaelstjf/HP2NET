#!/bin/env julia

# Argument usage:
# ARGS[1] = tree method (RAXML, IQTREE, MRBAYES)
# ARGS[2] = path of the tree
# ARGS[3] = path of the topology
# ARGS[4] = output dir
# ARGS[5] = num_workers
# ARGS[6] = hmax
# ARGS[7] = runs
# ARGS[8] = seed
# ARGS[9] = (optional) outgroup

#!/bin/env julia

using Random

println("Starting PhyloNetworks...")

if length(ARGS) < 8 || length(ARGS) > 9
    println("Usage: script.jl <tree method> <tree path> <topology path> <output dir> <num_workers> <hmax> <runs> <seed> [outgroup]")
    exit(1)
end

method = ARGS[1]
tree_path = ARGS[2]
topology_path = ARGS[3]
output_dir = ARGS[4]
num_workers = max(1, parse(Int, ARGS[5]) - 1)
hmax = parse(Int, ARGS[6])
runs = parse(Int, ARGS[7])
seed = parse(Int, ARGS[8])

if length(ARGS) == 8
    # Sem outgroup, seed é ARGS[8]
    outgroup = nothing
elseif length(ARGS) == 9
    # Com outgroup
    outgroup = ARGS[9]
end

println("Tree method: $method")
println("Tree path: $tree_path")
println("Topology path: $topology_path")
println("Output folder: $output_dir")
println("Number of processors: $num_workers")
println("Hybridization max: $hmax")
println("Number of runs: $runs")
if outgroup !== nothing
    println("Species mapping: $outgroup")
end
println("Using random seed: $seed")
import Pkg

# Ensure required packages are installed
function ensure_installed(pkgs)
    for pkg in pkgs
        if Base.find_package(pkg) === nothing
            println("Installing $pkg...")
            Pkg.add(pkg)
        end
    end
end

required_pkgs = ["PhyloNetworks", "Distributed", "CSV"]
ensure_installed(required_pkgs)

# Import packages
using PhyloNetworks
using Distributed
using CSV

if num_workers > 0
    addprocs(num_workers)
end

@everywhere using PhyloNetworks

basedir = dirname(output_dir)
name = string(replace(basename(basedir), "/" => ""), "_", method, "_MPL_", hmax)
output = joinpath(output_dir, name)

println("Using PhyloNetworks on every processor")

# Process different tree methods
if method in ["RAXML", "IQTREE"]
    if outgroup !== nothing
        genetrees = readMultiTopology(tree_path)
        taxon_map = Dict{String, String}()
        spec_list = split(outgroup, ';')
        for spec in spec_list
            sp = split(strip(spec), ':')
            for allele in split(sp[2], ',')
                taxon_map[allele] = sp[1]
            end
        end
        println(taxon_map)
        q, t = countquartetsintrees(genetrees, taxon_map)
        df_sp = writeTableCF(q, t)
        println(df_sp)
        CSV.write(joinpath(output_dir, "tableCF_species.csv"), df_sp)
        raxmlCF = readTableCF(joinpath(output_dir, "tableCF_species.csv"))
    else
        raxmlCF = readTrees2CF(tree_path, writeTab=false, writeSummary=false)
    end
    astraltree = readTopology(last(readlines(topology_path)))
    net = snaq!(astraltree, raxmlCF, hmax=hmax, filename=output, runs=runs, seed=seed)

elseif method == "MRBAYES"
    buckyCF = readTableCF(tree_path)
    qmc_tree = readTopology(topology_path)
    net = snaq!(qmc_tree, buckyCF, hmax=hmax, filename=output, runs=runs, seed=seed)

else
    println("Invalid tree method! Supported methods: RAXML, IQTREE, MRBAYES")
    exit(1)
end
