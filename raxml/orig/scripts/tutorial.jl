# julia script - PhyloNetworks wiki tutorial
#------------------------------------------------

# preparation: get useful packages
#---------------------------------

# with VS Code and the julialang extension, type:
# Command-Shift-P to open the Command Palette, then type "Julia: Start REPL"
# Control-Enter or Option-Enter to execute the current line of code into the REPL without copy-paste

# download and install packages if not done already:
# (do this once only)
using Pkg
Pkg.add("PhyloNetworks")
Pkg.add("PhyloPlots")
Pkg.add("CSV") # to read from / write to text files, e.g. csv files
Pkg.add("DataFrames") # to create & manipulate data frames
Pkg.add("RCall")

# load packages in memory (do every new session):
using PhyloNetworks
using PhyloPlots # to plot networks
using DataFrames # to read in data files directly

# first steps with julia and networks
#------------------------------------------------

pwd() ## we need to be where tutorial files are: baseline.gamma0.3_n30
## if necessary: change directory to get there,
## as below but adjust the path to whatever is the correct for you
cd("data_results/baseline.gamma0.3_n30")

# read/write
# note: the first time a function is called, it takes longer because it compiles
net = readTopology("(((A,(B)#H1:::0.9),(C,#H1:::0.1)),D);")
writeTopology(net)
? writeTopology # type this directly: "?" to go into help mode

# plot
plot(net, :R);

using RCall
R"pdf"("plot1.pdf", width=3, height=3);
plot(net, :R);
R"dev.off"();

; ls # type this directly: ";" to get into shell mode
; ls plot*

? plot
plot(net, :R, tipOffset = 0.3);
plot(net, :R, showEdgeNumber = true);

# root the network on a different outgroup
rootatnode!(net,"A")
# plot again to see new root
R"pdf"("plot2.pdf", width=3, height=3);
R"par"(mar=[0.5,0.5,0.5,0.5]); # to reduce margins
plot(net, :R, showEdgeNumber = true);
R"dev.off()";


# network estimation with SNaQ
#------------------------------------------------

buckyCF = readTableCF("bucky-output/1_seqgen.CFs.csv")
tre = readTopology("bucky-output/1_seqgen.QMC.tre")

net0 = snaq!(tre,  buckyCF, hmax=0, filename="snaq/net0_bucky", seed=123) # ~ 87 seconds
net1 = snaq!(net0, buckyCF, hmax=1, filename="snaq/net1_bucky", seed=456) # 360s on one instance
using Distributed
addprocs(4)
@everywhere using PhyloNetworks
net2 = snaq!(net1, buckyCF, hmax=2, filename="snaq/net2_bucky", seed=789) # 4 times faster

R"pdf"("plot3-net0.pdf", width=3, height=3);
plot(net0, :R);
R"dev.off()";

R"pdf"("plot4-net012.pdf", width=8, height=3); # to save as pdf if needed
R"layout(matrix(1:3,1,3))";
R"par"(mar=[0.1,0.1,0.1,0.5]);
rootatnode!(net0, "6")
plot(net0, :R);
rootatnode!(net1, "6")
plot(net1, :R);
rootatnode!(net2, "6")
plot(net2, :R);
R"dev.off"();

R"pdf"("plot5-net1.pdf", width=8, height=3); # to save as pdf if needed
R"layout"([1 2 3]);
R"par"(mar=[0.1,0.1,0.1,0.5]);
plot(net1, :R, showNodeNumber=true);
rotate!(net1, -3)
plot(net1, :R);
plot(net1, :R, showGamma=true);
R"dev.off"();

# output file
; cat snaq/net1_bucky.out
; cat snaq/net1_bucky.networks

# troubleshooting:
vnet = readMultiTopology("snaq/net1_bucky.networks")
plot(vnet[1], :R) # best network
rootatnode!(vnet[1],"6")
rootatnode!(vnet[2],"6")
R"pdf"("plot6-boot.pdf", width=6, height=3); # to save as pdf if needed
R"layout"([1 2]); # [1 2] is a 1x2 matrix. [1,2] is a vector of length 2
R"par"(mar=[0.1,0.1,0.1,0.5]);
plot(vnet[1], :R);
plot(vnet[2], :R);
R"dev.off"();
# look at the pdf: only different = gene flow direction

# to see their likelihood scores:
; less snaq/net1_bucky.networks
; grep -Eo "with -loglik [0-9]+.[0-9]+" snaq/net1_bucky.networks

# how to choose the complexity h: # reticulations
#------------------------------------------------

scores = [net0.loglik, net1.loglik, net2.loglik]
R"pdf"("score-vs-h.pdf", width=4, height=4);
R"plot"(x=0:2, y=scores, type="b", xlab="number of hybridizations h", ylab="network score");
R"dev.off"();

# bootstrap: here from bucky's credibility intervals for CFs
#------------------------------------------------

using CSV
buckyDat = CSV.read("bucky-output/1_seqgen.CFs.csv") # names like: CF12.34, CF12.34_lo etc.
rename!(buckyDat, Symbol("CF12.34") => :CF12_34)     # bootsnaq requires these colunm names
rename!(x -> Symbol(replace(String(x), "." => "_")), buckyDat) # do all in one go
bootnet = bootsnaq(net0, buckyDat, hmax=1, nrep=50, runs=3, filename="snaq/bootsnaq1_buckyCI")

bootnet = readMultiTopology("snaq/bootsnaq1_buckyCI.out")
length(bootnet)

# summarizing bootstrap. first, re-read net1 from file
net1 = readTopology("snaq/net1_bucky.out")
rootatnode!(net1, "6")
rotate!(net1, -4)

BSe_tree, tree1 = treeEdgesBootstrap(bootnet,net1)
plot(tree1, :R);
show(BSe_tree, allrows=true)
BSe_tree[BSe_tree[:proportion] .< 1.0, :]
plot(tree1, :R, edgeLabel=BSe_tree);
plot(net1,  :R, edgeLabel=BSe_tree);
plot(net1,  :R, edgeLabel=BSe_tree[BSe_tree[:proportion] .< 100.0, :]);


BSn, BSe, BSc, BSgam, BSedgenum = hybridBootstrapSupport(bootnet, net1);

# Percentage of bootstrap trees with an edge from the same sister clade to the same hybrid clade:
plot(net1, :R, edgeLabel=BSe[[:edge,:BS_hybrid_edge]]);
BSe # bootstrap frequencies associated to edges
BSc # makeup of all clades
BSc[:taxa][BSc[:c_14]] # list of taxa in new clade "c_14"
BSc[:taxa][BSc[:H7]]   # list of taxa in this clade

# Bootstrap support for the full reticulation relationships in the network, one at each hybrid node (support for same hybrid with same sister clades)
plot(net1, :R, nodeLabel=BSn[[:hybridnode,:BS_hybrid_samesisters]]);
# Bootstrap support for hybrid clades, shown on the parent edge of each node with positive hybrid support
plot(net1, :R, edgeLabel=BSn[BSn[:BS_hybrid].>0, [:edge,:BS_hybrid]]);
BSn # bootstrap frequencies associated to nodes


BSgam # array of gamma values
minimum(BSgam[:,2])
maximum(BSgam[:,2])
using Statistics
mean(BSgam[:,2])
std(BSgam[:,2])

# TICR test: does a given tree fit the CF data?
#------------------------------------------------

# TICR test: need to be in n15 folder
cd("../../data_results/n15.gamma0.30.20.2_n300")
pwd()
buckyCF = readTableCF("bucky-output/1_seqgen.CFs.csv") # read observed quartet CF
net0b = readTopology("snaq/net0_bucky.out") # tree estimated earlier with snaq!
net1b = readTopology("snaq/net1_bucky.out") # network estimated earlier
topologyQPseudolik!(net0b, buckyCF)         # update the fitted CFs under net0
fitCF0 = fittedQuartetCF(buckyCF, :long)    # extract them to a data frame
fitCF0
topologyQPseudolik!(net1b, buckyCF)         # update the fitted CFs under net1
fitCF1 = fittedQuartetCF(buckyCF, :long)

fitCF = rename(fitCF0, :expCF => :expCF_net0); # rename column "expCF" to "expCF_net0"
fitCF[:expCF_net1] = fitCF1[:expCF];           # add new column "expCF_net1"
CSV.write("snaq/fittedCF.csv", fitCF)

fitCF0[:h] = 0
fitCF1[:h] = 1
fitCF = vcat(fitCF0, fitCF1)
fitCF


using RCall
@rlibrary ggplot2
ggplot(fitCF, aes(x=:obsCF, y=:expCF)) + geom_point(alpha=0.1) +
  xlab("CF observed in gene trees") + ylab("CF expected under tree (h=0) or network (h=1)") +
  facet_grid(R"~h", labeller = label_both);
ggsave("CFfit.png", height=4, width=7)


## now, move to R
