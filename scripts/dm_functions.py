import os, sys, argparse, glob, tarfile
from pathlib import Path
from Bio import AlignIO

def remove_files_dir(folder):
    files = glob.glob(f'{folder}/*')
    try:
        for f in files:
            os.remove(f)
    except Exception:
        print(f"Error! Impossible to remove files from {folder}")

def create_folders(parent_dir, folder_list):
    for folder in folder_list:
        full_path = os.path.join(parent_dir, folder)
        try:
            Path(full_path).mkdir(exist_ok=True)
        except Exception:
            print(f'Impossible to create {full_path} folder')

def nexus_to_phylip(folder):
    path_ = os.path.dirname(folder)
    full_path = os.path.join(path_, "phylip")
    if not os.path.isdir(full_path):
        Path(full_path).mkdir(exist_ok=True)
        files = glob.glob(f'{folder}/*.nex')
        try:
            for f in files:
                out_name = os.path.basename(f).split('.')[0]
                AlignIO.convert(f, "nexus", os.path.join(full_path, f'{out_name}.phy'), "phylip-sequential")
        except Exception:
            print("Impossible to convert nexus files to phylip!")
    
def create_raxml_file(besttree_file):
    base_dir = os.path.dirname(besttree_file)
    raxml_dir = os.path.join(base_dir, 'raxml')
    bootstrap_dir = os.path.join(raxml_dir, "bootstrap")
    Path(bootstrap_dir).mkdir(exist_ok=True)
    # remove old files
    remove_files_dir(bootstrap_dir)
    # move the new files
    try:
        files = glob.glob(f'{raxml_dir}/RAxML_bootstrap.*')
        for f in files:
            os.rename(f, os.path.join(bootstrap_dir, os.path.basename(f)))
        # compress and remove the bootstrap files
        with tarfile.open(os.path.join(raxml_dir, "contrees.tgz"), "w:gz") as tar:
            files = glob.glob(f'{raxml_dir}/RAxML_bipartitions.*')
            for f in files:
                tar.add(f, arcname=os.path.basename(f))
            for f in files:
                os.remove(f)
    except Exception:
        print("Error! Directory does not exist or not enough privileges")
    #append all the besttrees into a single file(in the working dir), compress the files and remove them
    try:
        raxml_input = open(besttree_file, 'w+')
        files = glob.glob(os.path.join(raxml_dir, '/RAxML_bestTree.*'))
        trees = ""
        for f in files:
            gen_tree = open(f, 'r')
            trees += gen_tree.readline()
            gen_tree.close()
        raxml_input.write(trees)
        raxml_input.close()
        with tarfile.open(os.path.join(raxml_dir, "besttrees.tgz"), "w:gz") as tar:
            files = glob.glob(os.path.join(raxml_dir, '/RAxML_bestTree.*'))
            for f in files:
                tar.add(f, arcname=os.path.basename(f))
            for f in files:
                os.remove(f)
    except IOError:
        print("Error! Failed to create the input file")

def create_phylonet_input(input_, output, hmax, threads, runs):
    try:
        in_file = open(input_, 'r')
    except IOError:
        print("Error! Could not open input file")
        exit(1)
    try:
        out_file = open(output, 'w+')
    except IOError:
        print("Error! Could not open output file")
        exit(1)

    tree_index = 0
    buffer = "#NEXUS\nBEGIN TREES;\n"
    for tree in in_file.readlines():
        tree_index+=1
        buffer+="Tree geneTree" + str(tree_index) + " = " + tree
    in_file.close()
    buffer+='END;\nBEGIN PHYLONET;\nInferNetwork_MP ('

    for i in range(0, tree_index-1):
        buffer+="geneTree" + str(i+1) +','

    output_dir = os.path.dirname(output)
    output_network = os.path.join(output_dir, 'PhyloNet' + hmax + ".nex")
    buffer+="geneTree" + str(tree_index) +') ' + hmax + " -pl " + threads + " -x " + runs + " " + output_network + ';\nEND;'
    out_file.write(buffer)
    out_file.close()

def create_iqtree_file(besttree_file):
    base_dir = os.path.dirname(besttree_file)
    iqtree_dir = os.path.join(base_dir, 'iqtree')
    try:
        iq_input = open(besttree_file, 'w+')
        files = glob.glob(os.path.join(iqtree_dir, '/*.treefile'))
        trees = ""
        for f in files:
            gen_tree = open(f, 'r')
            trees += gen_tree.readline()
            gen_tree.close()
        iq_input.write(trees)
        iq_input.close()
    except IOError:
        print("Error! Failed to create the input file")

def clear_execution(network_method, tree_method, basedir):
    try:
        if tree_method == 'ML-RAXML':
            os.rmdir(os.path.join(basedir, 'raxml'))
        elif tree_method == 'ML-IQTREE':
            os.rmdir(os.path.join(basedir, 'iqtree'))
        elif tree_method == 'MP-TNT':
            os.rmdir(os.path.join(basedir, 'tnt'))
        elif tree_method == 'BI':
            pass
        if network_method == 'MPL':
            os.rmdir(os.path.join(basedir, 'astral'))
        elif network_method == 'MP':
            os.remove(os.path.join(basedir, '*.nex')) #input file
            pass
    except Exception:
        print("Error! Failed to clean temporary files")
