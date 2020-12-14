#!/bin/bash

RAXML_DIR=$1

mkdir -p $RAXML_DIR/bootstrap
rm -fr $RAXML_DIR/bootstrap/*

mv $RAXML_DIR/RAxML_bootstrap.* $RAXML_DIR/bootstrap
tar -czf $RAXML_DIR/contrees.tgz $RAXML_DIR/RAxML_bipartitions*
rm -f $RAXML_DIR/RAxML_bipartitions*
cat $RAXML_DIR/RAxML_bestTree.* > $RAXML_DIR/besttrees.tre
tar -czf $RAXML_DIR/besttrees.tgz $RAXML_DIR/RAxML_bestTree.*
rm -f $RAXML_DIR/RAxML_bestTree.*
