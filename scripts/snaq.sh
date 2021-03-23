#!/usr/bin/bash

source /scratch/app/modulos/julia-1.5.1.sh
export JULIA_PKGDIR=/scratch/cenapadrjsd/diego.carvalho/biocomp/julia
export JULIA_SYSIMAGE="--sysimage /scratch/cenapadrjsd/diego.carvalho/biocomp/julia/sys_PhyloNetworks.so"
export JULIA_SCRIPT="/scratch/cenapadrjsd/diego.carvalho/biocomp/scripts/snaq.jl"
export JULIA_NUM_THREADS=$2

cd $1
mkdir -p snaq
exec julia $JULIA_SYSIMAGE --threads $2 $JULIA_SCRIPT $2 $1