## continues from tutorial.jl for TICR test
## Claudia June 2016

devtools::install_github("lamho86/phylolm")
library(phylolm)
getwd() ## need to be in n15.gamma0.30.20.2_n300
dat = read.csv("snaq/fittedCF.csv")
head(dat)

layout(matrix(1:2,1,2))
plot(expCF_net0~obsCF, data=dat, xlab="CF observed in gene trees",
       ylab="CF fitted to a tree",         cex=0.5)
plot(expCF_net1~obsCF, data=dat, xlab="CF observed in gene trees",
       ylab="CF fitted to a network, h=1", cex=0.5)

par(mfrow=c(1,1)) ## change back to layout with 1 plot

## Reading the data
quartetCF = read.csv("bucky-output/1_seqgen.CFs.csv")
dim(quartetCF)
head(quartetCF)
dat = quartetCF[, c(1:4, 5, 8, 11)]
for (i in 1:4){ dat[,i] = factor(dat[,i])}
head(dat)

## Reading the tree
astraltree = read.tree("astral/astral.tre")[[102]]
astraltree
plot(astraltree) ## error!
plot(astraltree, use.edge.length=F) # no problem!
edgelabels(round(astraltree$edge.length,3))

astraltree = root(astraltree, "15")
plot(astraltree, use.edge.length=F)
edgelabels(round(astraltree$edge.length,3), frame="none", adj=c(0.5,0))

snaqtree = read.tree("snaq/net0_bucky.out")[[1]]
snaqtree = root(snaqtree, "15")
plot(snaqtree, use.edge.length=F)
edgelabels(round(snaqtree$edge.length,3), frame="none", adj=c(0.5,0))

## Preliminary calculations
astralprelim = test.tree.preparation(dat, astraltree)
Ntaxa = length(astraltree$tip.label) # 15 of course
Ntaxa
astral.internal.edges = which(astraltree$edge[,2] > Ntaxa)
astral.internal.edges

## Test of the coalescent on the tree
res <- test.one.species.tree(dat,astraltree,astralprelim,edge.keep=astral.internal.edges)
res[1:6]

## Detecting taxa involved in potential reticulations
outlier.4taxa.01 <- which(res$outlier.pvalues < 0.01)
length(outlier.4taxa.01)
q01 = as.matrix(quartetCF[outlier.4taxa.01,1:4])
q01

sort(table(as.vector(q01)),decreasing=TRUE)

cbind(
    dat[outlier.4taxa.01,],       # taxon names and observed CFs
    res$cf.exp[outlier.4taxa.01,] # CFs expected from the tree
 )

outlier.4taxa.05 <- which(res$outlier.pvalues < 0.05)
length(outlier.4taxa.05)
q05 = as.matrix(quartetCF[outlier.4taxa.05,1:4])
head(q05)
sort(table(as.vector(q05)),decreasing=TRUE)
sum(apply(q05,1,function(x){"1" %in% x  & "15" %in% x}))
sum(apply(q05,1,function(x){"10" %in% x & "11" %in% x}))

clade10.13 = c("10", "11", "12", "13")
table(apply(q05,1,function(x){length(intersect(x, clade10.13))}))
40+20+22
clade7.9 = c("7", "8", "9")
table(apply(q05,1,function(x){length(intersect(x, clade7.9))}))
