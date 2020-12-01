#!/usr/bin/env julia

# file "runSNaQ.jl". run in the shell like this in general:
# julia runSNaQ.jl hvalue nruns
# example for h=2 and default 10 runs:
# julia runSNaQ.jl 2
# or example for h=3 and 50 runs:
# julia runSNaQ.jl 3 50

length(ARGS) > 0 ||
    error("need 1 or 2 arguments: # reticulations (h) and # runs (optional, 10 by default)")
h = parse(Int, ARGS[1])
nruns = 10
if length(ARGS) > 1
    nruns = parse(Int, ARGS[2])
end
datafile = "bucky-output/1_seqgen.CFs.csv" # concordance factors from MrBayes + BUCKy
startfile = "astral/astral.tre" # use the astral tree to start the search if h=0 or 1
if h>1 # use the network with h-1 to search if h=2 or more 
    startfile = "snaq/net", h-1, "_bucky.out"
end
outputfile = string("snaq/net", h, "_bucky") # example: "net2_bucky"
seed = 7350 + h # different seeds for different h
info("will run SNaQ with h=",h,
    ", # of runs=",nruns,
    ", seed=",seed,
    ", output will go to: ", outputfile)

addprocs(nruns)
@everywhere using PhyloNetworks
using CSV
netstart = readTopology(startfile);
dat = CSV.read(datafile, categorical=false); # dat is a DataFrame
dCF = readTableCF!(dat);                     # dCF is of type DataCF
net = snaq!(netstart, dCF, hmax=h, filename=outputfile, seed=seed, runs=nruns)
