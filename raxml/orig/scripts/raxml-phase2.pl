#!/usr/bin/perl

# 2020 (@) Diego Carvalho - d.carvalho@ieee.org
# Perl script to run raxml + bootstrap for each locus sequence of commands. 
#
# adapted from https://github.com/crsl4/PhyloNetworks.jl/wiki

# May 26, 2016
# Perl script to run raxml + bootstrap for each locus.
# this is *not* part of the TICR pipeline
# adapted from Solis-Lemus, Yang and Ane (2016) scripts:
# https://github.com/crsl4/InconsistencySpeciesTreeGeneFlow/blob/master/scripts/estGeneTrees/raxml.pl

# model: HKY + Gamma (for rate variation across sites)

# usage:
#
# raxml.pl --seqdir=xxx/yyy --raxmldir=xxx --astraldir=xxx
#
# other options:
# --numCores = number of cores to use by RAxML. default 6
# --boot (default) or --noboot (not implemented!) to do RAxML bootstrap on each locus
# --numboot = number of bootstrap reps. default 100
# --conver2phylip (default) or --noconvert2phylip to convert nexus input files to phylip
# --doastral (default) or --nodoastral to do or not do ASTRAL at the end.
#
# the script will create a log file in raxmldir/raxml.pl.log

# warning: assuming all paths (seqdir and raxmldir) are relative paths, and linux/mac

use Getopt::Long;
use File::Path qw( make_path );
use strict;
use warnings;
use Carp;
#use lib '/u/c/l/claudia/lib/perl/lib/site_perl/'; # we need this because I had to install locally the Statistics module
#use Statistics::R;


# ================= parameters ======================

my $boot = 1; # boot=0 is not implemented, in fact!
my $numboot = 100;
my $numCores = 6;
my $seqdir;   # directory where sequences are
my $phylipdir;
my $raxmldir; # directory for output, including log for this script
my $astraldir;
my $convertphylip = 1;
my $doastral = 1;
# The raxml var points to a script that will store the execution that will be executed.
my $raxml = 'python3 ../scripts/raxmlinsert.py raxml'; # executable
# Point to the 'astral' tag. The java machine invocation is also changed to point to the script.
my $astral = 'astral'; # adapt to your system

# -------------- read arguments from command-line -----------------------
GetOptions( 'numboot=i' => \$numboot,
	    'boot!' => \$boot,
	    'numCores=i' => \$numCores,
	    'seqdir=s' => \$seqdir,
	    'raxmldir=s' => \$raxmldir,
	    'astraldir=s' => \$astraldir,
	    'convert2phylip!' => \$convertphylip,
	    'doastral!' => \$doastral,
    );

die "seqdir not defined or not a directory" if (!(defined $seqdir) or !(-d $seqdir));

#-----------------------------------------------#
#  restructure output files                     #
#-----------------------------------------------#

# archive phylip files
#`tar -czf ${phylipdir}.tgz $phylipdir`

# delete info files created by raxml
`rm $raxmldir/RAxML_info\.*`;

# create directory to contain the bootstrap trees for all genes: 1 file per gene
my $bootpath = "$raxmldir/bootstrap";
make_path $bootpath unless(-d $bootpath);
`mv $raxmldir/RAxML_bootstrap.* $bootpath`;
# do not tar: needed for ASTRAL

# create directory to contain the consensus trees: 2 files per gene
`tar -czf $raxmldir/contrees.tgz $raxmldir/RAxML_bipartitions*`;
`rm -f $raxmldir/RAxML_bipartitions*`;

# create file listing all best trees: one line per gene
my $raxmlOUT = "$raxmldir/besttrees.tre";
`cat $raxmldir/RAxML_bestTree\.* > $raxmlOUT`;
`tar -czf $raxmldir/besttrees.tgz $raxmldir/RAxML_bestTree\.*`;
`rm -f $raxmldir/RAxML_bestTree\.*`;

# ----------------------------------------------#
#   astral analysis                             #
# ----------------------------------------------#

$astraldir = "astral" if !defined($astraldir);
my $bsfile =  "$astraldir/BSlistfiles";
my $astralLOG =  "$astraldir/astral.screenlog";
my $astralOUT =  "$astraldir/astral.tre";

`ls -d $bootpath/* > $bsfile`;

#my $astralcmd = "java -jar $astral -i $raxmlOUT -b $bsfile -r $numboot -o $astralOUT > $astralLOG 2>&1";
my $astralcmd = "python3 ../scripts/raxmlinsert.py astral -jar $astral -i $raxmlOUT -b $bsfile -r $numboot -o $astralOUT > $astralLOG 2>&1";
open FHlog, ">> $logfile";
if ($doastral){
    print FHlog "running astral:\n";
} else {
    print FHlog "astral could be run with:\n";
}
print FHlog "$astralcmd\n";
close FHlog;
if ($doastral){
    system($astralcmd);
}
