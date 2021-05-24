import os, sys, argparse, glob, tarfile
from pathlib import Path
#get the files as arguments, hmax, threads and number of runs
parser = argparse.ArgumentParser(description='Input and output files.')
parser.add_argument("-i","--input", required=True, help='Name of the input file')
parser.add_argument("-o", "--output", required=True, help='Name of the output file')
parser.add_argument("-hm", "--hmax", required=True, help='Maximum number of hybridizations')
parser.add_argument("-t", "--threads", required=True, help='Number of threads')
parser.add_argument("-r", "--runs", required=True, help='Number of runs')
args = parser.parse_args()

#create the raxml input file from the input directory
raxml_dir = os.path.dirname(args.input)
bootstrap_dir = os.path.join(raxml_dir, "bootstrap")
Path(bootstrap_dir).mkdir(parents=True, exist_ok=True)
# remove old files
files = glob.glob(f'{bootstrap_dir}/*')
for f in files:
    os.remove(f)
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
    raxml_input = open(args.input, 'w+')
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

try:
    in_file = open(args.input, 'r')
except IOError:
    print("Error! Could not open input file")
    exit(1)
try:
    out_file = open(args.output, 'w+')
except IOError:
    print("Error! Could not open output file")
    exit(1)

tree_index = 0
buffer = "#NEXUS\nBEGIN TREES;\n"
for tree in in_file.readlines():
    tree_index+=1
    buffer+="geneTree" + str(tree_index) + " = " + tree
in_file.close()
buffer+='END;\nBEGIN PHYLONET;\nInferNetwork_MP ('

for i in range(0, tree_index-1):
    buffer+="geneTree" + str(i+1) +','

output_dir = os.path.dirname(args.output)
output_network = os.path.join(output_dir, 'PhyloNet' + args.hmax + ".nex")
buffer+="geneTree" + str(tree_index) +') ' + args.hmax + " -pl " + args.threads + " -x " + args.runs + " " + output_network + ';\nEND;'
out_file.write(buffer)
out_file.close()