import os, sys, argparse
#get the directories as arguments, hmax, threads and number of runs
parser = argparse.ArgumentParser(description='Input and output files.')
parser.add_argument("-i","--input", required=True, help='Name of the input file')
parser.add_argument("-o", "--output", required=True, help='Name of the output file')
parser.add_argument("-hm", "--hmax", required=True, help='Maximum number of hybridizations')
parser.add_argument("-t", "--threads", required=True, help='Number of threads')
parser.add_argument("-r", "--runs", required=True, help='Number of runs')
args = parser.parse_args()

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
buffer+='END;\nBEGIN PHYLONET;\nInferNetwork_ML ('

for i in range(0, tree_index-1):
    buffer+="geneTree" + str(i+1) +','

output_dir = os.path.dirname(args.output)
output_network = os.path.join(output_dir, 'PhyloNet' + args.hmax + ".nex")
buffer+="geneTree" + str(tree_index) +') ' + args.hmax + " -pl " + args.threads + " -x " + args.runs + " " + output_network + ';\nEND;'
out_file.write(buffer)
out_file.close()