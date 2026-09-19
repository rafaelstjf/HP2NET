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
# ARGS[9] = (optional) species mapping 

println("Starting PhyloNetworks...")

if length(ARGS) < 8 || length(ARGS) > 9
    println("""
    Usage:

      julia --threads=N snaq.jl \\
          <tree method> \\
          <tree path> \\
          <topology path> \\
          <output dir> \\
          <num_workers> \\
          <hmax> \\
          <runs> \\
          <seed> \\
          [species_mapping] \\

    Methods:

      RAXML
      IQTREE
      MRBAYES

    Example without mapping:

      julia --threads=8 script.jl \\
          RAXML trees.tre topology.tre output 8 3 32 123

    Example with mapping:

      julia --threads=8 script.jl \\
          RAXML trees.tre topology.tre output 8 3 32 123\\
          "DENV1:DENV1_001,DENV1_002;DENV2:DENV2_001,DENV2_002"
    """)
    exit(1)
end

method = uppercase(ARGS[1])
tree_path = ARGS[2]
topology_path = ARGS[3]
output_dir = ARGS[4]
num_workers = max(1, parse(Int, ARGS[5]))
hmax = parse(Int, ARGS[6])
runs = parse(Int, ARGS[7])
seed = parse(Int, ARGS[8])

if length(ARGS) < 8
    # without species mapping
    species_mapping = nothing
else
    # with species mapping
    species_mapping = ARGS[9]
end

println("Tree method: $method")
println("Tree path: $tree_path")
println("Topology path: $topology_path")
println("Output folder: $output_dir")
println("Number of processors: $num_workers")
println("Hybridization max: $hmax")
println("Number of runs: $runs")
println("Julia threads: $(Threads.nthreads())")
if outgroup !== nothing
    println("Allele/species mapping: $mapping")
else
    println("Allele/species mapping: disabled")
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

required_pkgs = ["PhyloNetworks", "SNaQ", "CSV", "DataFrames"]
ensure_installed(required_pkgs)

# Import packages
using Distributed

if nworkers() < num_workers
    addprocs(num_workers - nworkers())
end

@everywhere using PhyloNetworks
@everywhere using SNaQ
using CSV
using DataFrames
using Random


println("Distributed workers: $(nworkers())")
println("Julia threads: $(Threads.nthreads())")
println()

mkpath(output_dir)
basedir = dirname(output_dir)
name = string(
    replace(
        basename(basedir),
         "/" => ""
    ), "_", method, "_MPL_", hmax)
output = joinpath(output_dir, name)

println("Using PhyloNetworks on every processor")

#=
This function receives a String (mapping) and splits it in a dataframe
=#
function parse_species_mapping(mapping::String)
    allele = String[]
    species = String[]
    for entry in split(mapping, ';')
        entry = strip(entry)
        isempty(entry) && continue
        parts = split(
            entry,
            ':',
            limit=2
        )
        if length(parts) != 2
            error("Invalid species mapping entry: '$entry'\n" *
            "Expected format: species:allele1,allele2"
            )
        end
        sp = strip(parts[1])
        if isempty(sp)
            error("empty species name in mapping: '$entry'")
        end
        for al in split(parts[2], ',')
            al = strip(al)
            isempty(al) && continue
            push!(allele, al)
            push!(species, sp)
        end
    end
    if isempty(allele)
        error("The species mapping is empty.")
    end
    return DataFrame(allele=allele, species=species)
end

function calculate_species_cf(tree_path::String, output_dir::String, species_mapping)
    genetrees = readMultiTopology(tree_path)
    println(
        "Calculating quartet concordance factors " *
        "using $(Threads.nthreads()) Julia threads..."
    )
    q, t = countquartetsintrees(genetrees)
    df_cf = writeTableCF(q, t)
    if species_mapping === nothing
        cf_file = joinpath(output_dir, "tableCF.csv")
        CSV.write(cf_file, df_cf)
        return readTableCF(df_cf)
    end
    mapping_df = parse_species_mapping(species_mapping)
    mapping_file = joinpath(output_dir, "allele_species_mapping.csv")
    CSV.write(mapping_file, mapping_df)
    SNaQ.mapallelesCFtable(mapping_df, df_cf)
    mapped_cf_file = joinpath(output_dir, "tableCF_species.csv")
    CSV.write(mapped_cf_file, df_cf)
    dataCF = readtableCF(df_cf; mergerows=true)
    return dataCF

# Process different tree methods
if method in ["RAXML", "IQTREE"]
    raxmlCF = calculate_species_cf(tree_path, output_dir, species_mapping)
    topology_lines = readlines(topology_path)
    if isempty(topology_lines)
        error("Topology file is empty: " * topology_path)
    end
    astraltree = readTopology(last(topology_lines))
    net = snaq!(astraltree, raxmlCF, hmax=hmax, filename=output, runs=runs, seed=seed)
elseif
    buckyCF = readtableCF(tree_path)
    topology_lines = readlines(topology_path)
    if isempty(topology_lines)
        error("Topology file is empty: " * topology_path)
    end
    qmc_tree = readTopology(last(topology_lines))
    net = snaq!(qmc_tree, buckyCF, hmax=hmax, filename=output, runs=runs, seed=seed)
end
else
    error("Invalid tree method '$method'.")
end
println("Output prefix:")
println(output)
println("Estimated network:")
println(net)