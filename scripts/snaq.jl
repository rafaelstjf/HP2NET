using PhyloNetworks
using PhyloPlots
using Distributed


println("Julia SNAQ Scrip Starting")

addprocs(parse(Int64,ARGS[1]) - 1)

numproc = Threads.nthreads()

println("Julia Threads: ", numproc)

println("Issuing using PhyloNetworks on every thread")
@everywhere using PhyloNetworks
@everywhere using PhyloPlots

println("Reading AstralTree")
astraltree = readMultiTopology("astral/astral.tre")

println("Reading raxmlCF")
raxmlCF = readTrees2CF("raxml/besttrees.tre",writeTab=false, writeSummary=false)

println("Calculating SNAQ...")
net0 = snaq!(last(astraltree), raxmlCF, hmax=0, filename="snaq/net0_astral", seed=123, runs=numproc)
net1 = snaq!(net0, raxmlCF, hmax=1, filename="snaq/net1_astral", seed=456, runs=numproc)
net2 = snaq!(net1, raxmlCF, hmax=2, filename="snaq/net2_astral", seed=789, runs=numproc)

#println("Ploting...")
#R"pdf"("plot3-net0.pdf", width=3, height=3);
#plot(net0, :R);
#R"dev.off()";
