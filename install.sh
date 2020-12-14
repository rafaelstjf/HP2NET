#!/bin/bash

JOBDIR=/scratch/cenapadrjsd/diego.carvalho/biocomp
 
mkdir -p $JOBDIR/tmp
cp -R Astral setup_astral_data.sh parsl.env work.config parsl_inside_allocation.py raxml-phase1.pl workflow.py submit.sh $JOBDIR
