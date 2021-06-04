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

def nexus_to_phylip(folder):
    path_ = os.path.dirname(folder)
    full_path = os.path.join(path_, "phylip")
    Path(full_path).mkdir(exist_ok=True)
    remove_files_dir(full_path)
    files = glob.glob(f'{folder}/*.nex')
    try:
        for f in files:
            out_name = os.path.basename(f).split('.')[0]
            AlignIO.convert(f, "nexus", os.path.join(full_path, f'{out_name}.phy'), "phylip-sequential")
    except Exception:
        print("Impossible to convert nexus files to phylip!")
    

def create_raxml_file(input_):
    #create the raxml input_ file from the input directory
    raxml_dir = os.path.dirname(input_)
    bootstrap_dir = os.path.join(raxml_dir, "bootstrap")
    Path(bootstrap_dir).mkdir(exist_ok=True)
    # remove old files
    remove_files_dir(bootstrap_dir)
    # move the new files
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

    #append all the besttrees into a single file, compress the files and remove them
    try:
        raxml_input = open(input_, 'w+')
        files = glob.glob(f'{raxml_dir}/RAxML_bestTree.*')
        trees = ""
        for f in files:
            gen_tree = open(f, 'r')
            trees += gen_tree.readline()
            gen_tree.close()
        raxml_input.write(trees)
        raxml_input.close()
        with tarfile.open(os.path.join(raxml_dir, "besttrees.tgz"), "w:gz") as tar:
            files = glob.glob(f'{raxml_dir}/RAxML_bestTree.*')
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



def main():
    #get the files as arguments, hmax, threads and number of runs
    parser = argparse.ArgumentParser(description='Input and output files.')
    parser.add_argument("-i","--input", required=True, help='Name of the input file')
    parser.add_argument("-o", "--output", required=True, help='Name of the output file')
    parser.add_argument("-hm", "--hmax", required=True, help='Maximum number of hybridizations')
    parser.add_argument("-t", "--threads", required=True, help='Number of threads')
    parser.add_argument("-r", "--runs", required=True, help='Number of runs')
    args = parser.parse_args()
    create_raxml_file(args.input)
    create_phylonet_input(args.input, args.output, args.hmax, args.threads, args.runs)

if __name__ == '__main__':
    main()