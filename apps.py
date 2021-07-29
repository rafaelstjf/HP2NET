# -*- coding: utf-8 -*-

""" Apps.py. Parsl Application Functions (@) 2021

This module encapsulates all Parsl configuration stuff in order to provide a
cluster configuration based in number of nodes and cores per node.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

# COPYRIGHT SECTION
__author__ = "Diego Carvalho"
__copyright__ = "Copyright 2021, The Biocomp Informal Collaboration (CEFET/RJ and LNCC)"
__credits__ = ["Diego Carvalho", "Carla Osthoff", "Kary Ocaña", "Rafael Terra"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Diego Carvalho"
__email__ = "d.carvalho@ieee.org"
__status__ = "Research"


#
# Parsl Bash and Python Applications
#
from pandas.core import base
import parsl
from bioconfig import BioConfig


# setup_phylip_data bash app
@parsl.python_app(executors=['single_thread'])
def setup_phylip_data(basedir: str, config: BioConfig,
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Extract the sequence alignments tar file and convert the gene alignments from the nexus format to the phylip format.

    Parameters:
            basedir: it is going to search for a tar file with nexus files. The script will create:
            seqdir=input/nexus
            seqdir=input/phylip
    Returns:
        returns an parsl's AppFuture.

    Raises:
        PhylipMissingData --- if cannot find a tar file with nexus files.


    TODO: Provide provenance.
    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os, glob, tarfile
    from Bio import AlignIO
    from pathlib import Path
    from appsexception import PhylipMissingData

    input_dir = os.path.join(basedir, 'input')
    input_nexus_dir = os.path.join(input_dir, 'nexus')
    # So, some work must be done. Build the Nexus directory
    if not os.path.isdir(input_nexus_dir):
        os.mkdir(input_nexus_dir)
        # List all tar.gz files, they are supposed to be the input
        tar_file_list = glob.glob(f'{input_dir}/*.tar.gz')
        if len(tar_file_list) == 0:
            raise PhylipMissingData(input_dir)
        # So, loop over and untar every file
        for tar_file in tar_file_list:
            os.system(f'cd {input_nexus_dir}; tar zxvf {tar_file}')
    # Now, use the function to convert nexus to phylip.
    input_phylip_dir = os.path.join(input_dir, "phylip")
    if not os.path.isdir(input_phylip_dir):
        Path(input_phylip_dir).mkdir(exist_ok=True)
        files = glob.glob(f'{input_nexus_dir}/*.nex')
        try:
            for f in files:
                out_name = os.path.basename(f).split('.')[0]
                AlignIO.convert(f, "nexus", os.path.join(input_phylip_dir, f'{out_name}.phy'), "phylip-sequential")
        except Exception:
            print("Impossible to convert nexus files to phylip!")
    return


# raxml bash app
@parsl.bash_app(executors=['tree_and_statistics'])
def raxml(basedir: str, config: BioConfig,
          input_file: str,
          inputs=[],
          stderr=parsl.AUTO_LOGNAME,
          stdout=parsl.AUTO_LOGNAME):
    """Runs the Raxml's executable on a sequence alignment in phylip format
    Parameters:
        basedir: current working directory
        input_file: a sequence alignment in phylip format

    Returns:
        an parsl's AppFuture
    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import random
    import logging

    num_threads = config.raxml_threads
    raxml_exec = config.raxml
    exec_param = config.raxml_exec_param

    logging.info(f'raxml called with {basedir}')
    raxml_dir = basedir + '/' + config.raxml_dir

    # TODO: Create the following parameters(external configuration): -m, -N,
    flags = f"-T {num_threads} {exec_param}"

    p = random.randint(1, 10000)
    x = random.randint(1, 10000)

    output_file = os.path.basename(input_file).split('.')[0]

    # Return to Parsl to be executed on the workflow
    return f"cd {raxml_dir}; {raxml_exec} {flags} -p {p} -x {x} -s {input_file} -n {output_file}"


@parsl.python_app(executors=['single_thread'])
def setup_tree_output(basedir: str,
                      config: BioConfig,
                      inputs=[],
                      outputs=[],
                      stderr=parsl.AUTO_LOGNAME,
                      stdout=parsl.AUTO_LOGNAME):
    """Create the phylogenetic tree software (raxml, iqtree,...) best tree file and organize the temporary files to subsequent softwares 

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: 
        Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os, glob, tarfile
    from pathlib import Path
    if(config.tree_method == "ML_RAXML"):
        raxml_dir = os.path.join(basedir, config.raxml_dir)
        bootstrap_dir = os.path.join(raxml_dir, "bootstrap")
        besttree_file = os.path.join(raxml_dir, config.raxml_output)
        try:
            Path(bootstrap_dir).mkdir(exist_ok=True)
        except Exception:
            print("Failed to create the raxml bootstrap folder!")
        old_files = glob.glob(f'{bootstrap_dir}/*')
        try:
            for f in old_files:
                os.remove(f)
        except Exception:
            print(f"Error! Impossible to remove files from {old_files}")
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
            raxml_input = open(besttree_file, 'w')
            files = glob.glob(os.path.join(raxml_dir, 'RAxML_bestTree.*'))
            trees = ""
            for f in files:
                gen_tree = open(f, 'r')
                trees += gen_tree.readline()
                gen_tree.close()
            raxml_input.write(trees)
            raxml_input.close()
            with tarfile.open(os.path.join(raxml_dir, "besttrees.tgz"), "w:gz") as tar:
                files = glob.glob(os.path.join(raxml_dir, 'RAxML_bestTree.*'))
                for f in files:
                    tar.add(f, arcname=os.path.basename(f))
                for f in files:
                    os.remove(f)
        except IOError:
            print("Error! Failed to create the input file")
    elif(config.tree_method == "ML_IQTREE"):
        phylip_dir = os.path.join(basedir, os.path.join("input", "phylip"))
        iqtree_dir = os.path.join(basedir, config.iqtree_dir)
        besttree_file = os.path.join(iqtree_dir, config.iqtree_output)
        try:
            files = glob.glob(os.path.join(phylip_dir, '*.iqtree'))
            files += glob.glob(os.path.join(phylip_dir, '*.treefile'))
            files += glob.glob(os.path.join(phylip_dir, '*.mldist'))
            files += glob.glob(os.path.join(phylip_dir, '*.nex'))
            files += glob.glob(os.path.join(phylip_dir, '*.contree'))
            files += glob.glob(os.path.join(phylip_dir, '*.log'))
            files += glob.glob(os.path.join(phylip_dir, '*.ckp.gz'))
            files += glob.glob(os.path.join(phylip_dir, '*.bionj'))
            files += glob.glob(os.path.join(phylip_dir, '*.reduced'))
            files += glob.glob(os.path.join(phylip_dir, '*.boottrees'))
            for f in files:
                new_f = os.path.join(iqtree_dir, os.path.basename(f))
                os.replace(f, new_f)
            iq_input = open(besttree_file, 'w+')
            files = glob.glob(os.path.join(iqtree_dir, '*.treefile'))
            trees = ""
            for f in files:
                gen_tree = open(f, 'r')
                trees += gen_tree.readline()
                gen_tree.close()
            iq_input.write(trees)
            iq_input.close()
        except IOError:
            print("Error! Failed to create the input file")
    return

@parsl.bash_app(executors=['single_thread'])
def astral(basedir: str,
           config: BioConfig,
           inputs=[],
           outputs=[],
           stderr=parsl.AUTO_LOGNAME,
           stdout=parsl.AUTO_LOGNAME):
    """Runs the Astral's executable using the besttree file and create the species tree in the astral folder

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import glob
    import os
    astral_dir = f"{basedir}/{config.astral_dir}"
    bs_file = f'{astral_dir}/BSlistfiles'
    exec_astral = config.astral
    tree_output = ""
    if(config.tree_method == "ML_RAXML"):
        raxm_dir = os.path.join(basedir, config.raxml_dir)
        tree_output = os.path.join(raxm_dir,config.raxml_output)
        boot_strap = f"{basedir}/{config.raxml_dir}/bootstrap/*"
        with open(bs_file, 'w') as f:
            for i in glob.glob(boot_strap):
                f.write(f'{i}\n')
    elif(config.tree_method == "ML_IQTREE"):
        iqtree_dir = os.path.join(basedir, config.iqtree_dir)
        tree_output = os.path.join(iqtree_dir,config.iqtree_output)
        boot_strap = f"{basedir}/{config.iqtree_dir}/*.boottrees"
        with open(bs_file, 'w') as f:
            for i in glob.glob(boot_strap):
                f.write(f'{i}\n')
    astral_output = os.path.join(astral_dir, config.astral_output)
    # Return to Parsl to be executed on the workflow
    return f'{exec_astral} -i {tree_output} -b {bs_file} {config.astral_exec_param} -o {astral_output}'

@parsl.bash_app(executors=['phylogenetic_network'])
def snaq(basedir: str,
         config: BioConfig,
         inputs=[],
         outputs=[],
         stderr=parsl.AUTO_LOGNAME,
         stdout=parsl.AUTO_LOGNAME):
    """Runs the phylonetwork algorithm (snaq) and create the phylogenetic network in newick format

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    # set environment variables
    import os
    from pathlib import Path
    os.environ["JULIA_SETUP"] = config.julia_setup
    #os.environ["JULIA_PKGDIR"] = config.julia_pkgdir
    #os.environ["JULIA_SYSIMAGE"] = config.julia_sysimage
    # run the julia script with PhyloNetworks
    snaq_exec = config.snaq
    num_threads = config.snaq_threads
    output_folder = os.path.join(basedir, 'snaq')
    hmax = config.snaq_hmax
    sysimage = ""
    if(config.julia_sysimage != ""):
        sysimage = f'{config.julia_sysimage} '
    if config.tree_method == "ML_RAXML":
        raxml_tree = os.path.join(os.path.join(basedir, config.raxml_dir), config.raxml_output)
        astral_tree = os.path.join(basedir, config.astral_dir)
        astral_tree = os.path.join(astral_tree, config.astral_output)
        return f'julia {sysimage}--threads {num_threads} {snaq_exec} 0 {raxml_tree} {astral_tree} {output_folder} {num_threads} {hmax}'
    elif config.tree_method == "ML_IQTREE":
        iqtree_tree = os.path.join(os.path.join(basedir, config.iqtree_dir), config.iqtree_output)
        astral_tree = os.path.join(basedir, config.astral_dir)
        astral_tree = os.path.join(astral_tree, config.astral_output)
        return f'julia {sysimage}--threads {num_threads} {snaq_exec} 0 {iqtree_tree} {astral_tree} {output_folder} {num_threads} {hmax}'
    elif config.tree_method == "BI_MRBAYES":
        dir_name = os.path.basename(basedir)
        qmc_output = os.path.join(os.path.join(basedir, config.quartet_maxcut_dir), f'{dir_name}.tre')
        bucky_folder = os.path.join(basedir, config.bucky_dir)
        bucky_table = os.path.join(bucky_folder, f"{dir_name}.csv")
        return f'julia {sysimage}--threads {num_threads} {snaq_exec} 1 {bucky_table} {qmc_output} {output_folder} {num_threads} {hmax}'
    else:
        return

# Mr.Bayes bash app


@parsl.bash_app(executors=['single_thread'])
def mrbayes(basedir: str,
            config: BioConfig,
            input_file: str,
            inputs=[],
            stderr=parsl.AUTO_LOGNAME,
            stdout=parsl.AUTO_LOGNAME):
    """Runs the Mr. Bayes' executable on a sequence alignment file

    Parameters:
        input_file
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    from pathlib import Path
    gene_name = os.path.basename(input_file)
    mb_folder = os.path.join(basedir, config.mrbayes_dir)
    gene_file = open(input_file, 'r')
    gene_string = gene_file.read()
    gene_file.close()
    # open the gene alignment file, read its contents and create a new file with mrbayes parameters
    gene_par = open(os.path.join(mb_folder, gene_name), 'w+')
    gene_par.write(gene_string)
    par = f"begin mrbayes;\nset nowarnings=yes;\nset autoclose=yes;\nlset nst=2;\n{config.mrbayes_parameters};\nmcmc;\nsumt;\nend;"
    gene_par.write(par)
    return f"{config.mrbayes} {os.path.join(mb_folder, gene_name)} 2>&1 | tee {os.path.join(mb_folder, gene_name + '.log')}"

# mbsum bash app


@parsl.bash_app(executors=['single_thread'])
def mbsum(basedir: str,
          config: BioConfig,
          input_file: str,
          inputs=[],
          stderr=parsl.AUTO_LOGNAME,
          stdout=parsl.AUTO_LOGNAME):
    """Runs the mbsum's executable on the Mr.Bayes output for a certain sequence alignment

    Parameters:
        basedir: current working directory
        input_file: a sequence alignment in nexus format
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    from pathlib import Path
    import re
    import glob
    gene_name = os.path.basename(input_file)
    mbsum_folder = os.path.join(basedir, config.mbsum_dir)
    mrbayes_folder = os.path.join(basedir, config.mrbayes_dir)
    # get the mrbayes parameters
    par_0 = re.sub("mcmcp ", "", config.mrbayes_parameters)
    par = par_0.split(' ')
    par_dir = {}
    for p in par:
        p_split = p.split('=')
        par_dir[p_split[0]] = float(p_split[1])
    trim = (((par_dir['ngen']/par_dir['samplefreq']) *
            par_dir['nruns']*par_dir['burninfrac'])/par_dir['nruns']) + 1
    # select all the mrbayes .t files of the gene alignment file
    trees = glob.glob(os.path.join(mrbayes_folder, gene_name + '*.t'))
    return f"{config.mbsum} {(' ').join(trees)} -n {trim} -o {os.path.join(mbsum_folder, gene_name + '.sum')}"


@parsl.python_app(executors=['single_thread'])
def setup_bucky_data(basedir: str,
                     config: BioConfig,
                     inputs=[],
                     stderr=parsl.AUTO_LOGNAME,
                     stdout=parsl.AUTO_LOGNAME):
    """Prepares bucky's input, creating a prune tree for each selected quartet

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import re
    import os
    from pathlib import Path
    import glob
    from itertools import combinations
    mbsum_folder = os.path.join(basedir, config.mbsum_dir)
    bucky_folder = os.path.join(basedir, config.bucky_dir)
    # parse the sumarized taxa by mbsum
    files = glob.glob(os.path.join(mbsum_folder, '*.sum'))
    taxa = {}
    selected_taxa = {}
    pattern = re.compile('translate(\n\s*\d+\s+\w+(,|;))+')
    taxa_pattern = re.compile('(\w+(,|;))')
    for file in files:
        gene_sum = open(file, 'r')
        text = gene_sum.read()
        gene_sum.close()
        translate_block = pattern.search(text)
        for match in re.findall(taxa_pattern, translate_block[0]):
            key = re.sub(re.compile('(,|;)'), '', match[0])
            if key in taxa:
                taxa[key] += 1
            else:
                taxa[key] = 1
    # select the taxa shared across all genes
    for t in taxa:
        if(taxa[t] == len(files)):
            selected_taxa[t] = t
    # create all the selected quartets combinations
    quartets = combinations(selected_taxa, 4)
    for quartet in quartets:
        prune_tree_output = "translate\n"
        count = 0
        filename = ""
        for member in tuple(quartet):
            filename += member
            count += 1
            prune_tree_output += f" {count} {member}"
            if count == 4:
                prune_tree_output += ";\n"
            else:
                filename += "--"
                prune_tree_output += ",\n"
        # create the prune tree file necessary for bucky
        prune_file_path = os.path.join(bucky_folder, f"{filename}-prune.txt")
        output_file = os.path.join(bucky_folder, filename)
        prune_file = open(prune_file_path, 'w')
        prune_file.write(prune_tree_output)
        prune_file.close()
    return


@parsl.bash_app(executors=['single_thread'])
def bucky(basedir: str,
          config: BioConfig,
          prune_file: str,
          inputs=[],
          outputs=[],
          stderr=parsl.AUTO_LOGNAME,
          stdout=parsl.AUTO_LOGNAME):
    """Runs bucky's executable using as input a certain prune tree file

    Parameters:
        basedir: current working directory
        prune_file:  prune tree file
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import glob
    import re
    mbsum_folder = os.path.join(basedir, config.mbsum_dir)
    bucky_folder = os.path.join(basedir, config.bucky_dir)
    files = glob.glob(os.path.join(mbsum_folder, '*.sum'))
    output_file = os.path.basename(prune_file)
    output_file = re.sub("-prune.txt", "", output_file)
    output_file = os.path.join(bucky_folder, output_file)
    return f"{config.bucky} -a 1 -n 1000000 -cf 0 -o {output_file} -p {prune_file} {(' ').join(files)}"


@parsl.python_app(executors=['single_thread'])
def setup_bucky_output(basedir: str,
                       config: BioConfig,
                       inputs=[],
                       outputs=[],
                       stderr=parsl.AUTO_LOGNAME,
                       stdout=parsl.AUTO_LOGNAME):
    """Create the Concordance Factor table using bucky's outputs

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import re
    import os
    import glob
    bucky_folder = os.path.join(basedir, config.bucky_dir)
    pattern = re.compile("Read \d+ genes with a ")
    out_files = glob.glob(os.path.join(bucky_folder, "*.out"))
    table_string = "taxon1,taxon2,taxon3,taxon4,CF12.34,CF12.34_lo,CF12.34_hi,CF13.24,CF13.24_lo,CF13.24_hi,CF14.23,CF14.23_lo,CF14.23_hi,ngenes\n"
    cf_95_pattern = re.compile("(95% CI for CF = \(\w+,\w+\))")
    mean_num_loci_pattern = re.compile("(=\s+\d+\.\d+\s+\(number of loci\))")
    translate_block_pattern = re.compile("translate\n(\s*\w+\s*\w+(,|;)\n*)+")
    # open all the bucky's output files and parse them
    for out_file in out_files:
        taxa = []
        splits = {}
        f = open(out_file, 'r')
        lines = f.read()
        f.close()
        num_genes = re.search(pattern, lines).group(0)
        num_genes = re.search("\d+", num_genes).group(0)
        name_wo_extension = re.sub(".out|", "", os.path.basename(out_file))
        concordance_file = os.path.join(os.path.dirname(
            out_file), f"{name_wo_extension}.concordance")
        f = open(concordance_file, 'r')
        lines = f.read()
        f.close()
        translate_block = re.search(translate_block_pattern, lines).group(0)
        translate_block = re.sub("(,|;|translate\n)", "", translate_block)
        taxon_list = translate_block.split('\n')
        for taxon in taxon_list:
            if(taxon == ""):
                break
            t = taxon.split(" ")
            taxa.append(t[2])
        all_splits_block = lines.split("All Splits:\n")[1]
        split = re.findall("{\w+,\w+\|\w+,\w+}", all_splits_block)
        cf = re.findall(mean_num_loci_pattern, all_splits_block)
        cf_95 = re.findall(cf_95_pattern, all_splits_block)
        for i in range(0, len(split)):
            split[i] = re.sub("({|,|})", "", split[i])
            split_dict = {}
            cf[i] = re.sub("(=|\(number of loci\)|\s+)", "", cf[i])
            cf_95[i] = re.sub("(95% CI for CF = \(|\))", "", cf_95[i])
            cf_95_list = cf_95[i].split(',')
            print(cf_95_list)
            split_dict['CF'] = float(cf[i])/float(num_genes)
            split_dict['95_CI_LO'] = float(cf_95_list[0])/float(num_genes)
            split_dict['95_CI_HI'] = float(cf_95_list[1])/float(num_genes)
            splits[split[i]] = split_dict
        parsed_line = (',').join(taxa)
        parsed_line += ','
        if "12|34" in splits:
            parsed_line += f"{splits['12|34']['CF']},{splits['12|34']['95_CI_LO']},{splits['12|34']['95_CI_HI']},"
        else:
            parsed_line += "0,0,0,"
        if "13|24" in splits:
            parsed_line += f"{splits['13|24']['CF']},{splits['13|24']['95_CI_LO']},{splits['13|24']['95_CI_HI']},"

        else:
            parsed_line += "0,0,0,"
        if "14|23" in splits:
            parsed_line += f"{splits['14|23']['CF']},{splits['14|23']['95_CI_LO']},{splits['14|23']['95_CI_HI']}"
        else:
            parsed_line += "0,0,0"
        parsed_line += f",{num_genes}\n"
        table_string += parsed_line
    # create the table folder
    table_name = os.path.basename(basedir)
    table_name = os.path.join(bucky_folder, f"{table_name}.csv")
    table_file = open(table_name, 'w')
    table_file.write(table_string)
    table_file.close()
    return


@parsl.python_app(executors=['single_thread'])
def setup_qmc_data(basedir: str,
                   config: BioConfig,
                   inputs=[],
                   outputs=[],
                   stderr=parsl.AUTO_LOGNAME,
                   stdout=parsl.AUTO_LOGNAME):
    """Prepares the Quartet MaxCut input

    Parameters:
        basedir: current working directory    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import pandas as pd
    import json
    import os
    from pathlib import Path
    dir_name = os.path.basename(basedir)
    bucky_folder = os.path.join(basedir, config.bucky_dir)
    table_filename = os.path.join(bucky_folder, f'{dir_name}.csv')
    try:
        table = pd.read_csv(table_filename, delimiter=',', dtype='string')
    except Exception:
        print("Failed to open CF table")
    table = pd.read_csv(table_filename, delimiter=',', dtype='string')
    # parse the table
    quartets = []
    taxa = {}
    for index, row in table.iterrows():
        for i in range(1, 5):
            if row['taxon' + str(i)] in taxa:
                taxa[row['taxon' + str(i)]] += 1
            else:
                taxa[row['taxon' + str(i)]] = 1
        cf = {'CF12.34': float(row['CF12.34']), 'CF13.24': float(
            row['CF13.24']), 'CF14.23': float(row['CF14.23'])}
        cf_sorted = [k for k in sorted(cf, key=cf.get, reverse=True)]
        cf_1 = row[cf_sorted[0]]
        cf_2 = row[cf_sorted[1]]
        cf_3 = row[cf_sorted[2]]
        split_1 = f"{row['taxon' + cf_sorted[0][2]]},{row['taxon' + cf_sorted[0][3]]}|{row['taxon' + cf_sorted[0][5]]},{row['taxon' + cf_sorted[0][6]]}"
        split_2 = f"{row['taxon' + cf_sorted[1][2]]},{row['taxon' + cf_sorted[1][3]]}|{row['taxon' + cf_sorted[1][5]]},{row['taxon' + cf_sorted[1][6]]}"
        split_3 = f"{row['taxon' + cf_sorted[2][2]]},{row['taxon' + cf_sorted[2][3]]}|{row['taxon' + cf_sorted[2][5]]},{row['taxon' + cf_sorted[2][6]]}"
        if(cf_1 == cf_2 == cf_3):
            quartets.extend([split_1, split_2, split_3])
        elif (cf_1 == cf_2):
            quartets.extend([split_1, split_2])
        else:
            quartets.append(split_1)
    # change taxon names to ids
    taxa_id = 1
    taxon_to_id = {}
    id_to_taxon = {}
    dir_name = os.path.basename(basedir)
    for k in sorted(taxa):
        taxon_to_id[str(taxa_id)] = k
        id_to_taxon[k] = taxa_id
        taxa_id += 1
    for i in range(0, len(quartets)):
        tmp1 = quartets[i].split('|')
        old_quartets = []
        old_quartets.extend(tmp1[0].split(','))
        old_quartets.extend(tmp1[1].split(','))
        quartets[i] = f"{id_to_taxon[old_quartets[0]]},{id_to_taxon[old_quartets[1]]}|{id_to_taxon[old_quartets[2]]},{id_to_taxon[old_quartets[3]]}"
    qmc_folder = os.path.join(basedir, "qmc")
    qmc_input = os.path.join(qmc_folder, f'{dir_name}.txt')
    qmc_input_file = open(qmc_input, 'w+')
    qmc_input_file.write((' ').join(quartets))
    qmc_input_file.close()
    # dump ids
    with open(os.path.join(qmc_folder, f'{dir_name}.json'), "w+") as outfile:
        json.dump(taxon_to_id, outfile)
    return


@parsl.bash_app(executors=['single_thread'])
def quartet_maxcut(basedir: str,
                   config: BioConfig,
                   inputs=[],
                   outputs=[],
                   stderr=parsl.AUTO_LOGNAME,
                   stdout=parsl.AUTO_LOGNAME):
    """Runs quartet maxcut's executable
    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    dir_name = os.path.basename(basedir)
    qmc_folder = os.path.join(basedir, "qmc")
    qmc_input = os.path.join(qmc_folder, f'{dir_name}.txt')
    qmc_output = os.path.join(qmc_folder, f'{dir_name}.tre')
    exec_qmc = os.path.join(
        config.quartet_maxcut_exec_dir, config.quartet_maxcut)
    return f'{exec_qmc} qrtt={qmc_input} otre={qmc_output}'


@parsl.python_app(executors=['single_thread'])
def setup_qmc_output(basedir: str,
                     config: BioConfig,
                     inputs=[],
                     outputs=[],
                     stderr=parsl.AUTO_LOGNAME,
                     stdout=parsl.AUTO_LOGNAME):
    """Prepare quartet maxcut's output file

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    import pandas as pd
    import re
    import json
    dir_name = os.path.basename(basedir)
    qmc_folder = os.path.join(basedir, "qmc")
    qmc_output = os.path.join(qmc_folder, f'{dir_name}.tre')
    taxon_json = os.path.join(qmc_folder, f'{dir_name}.json')
    with open(taxon_json, 'r') as f:
        taxon_to_id = json.load(f)
    tree_file = open(qmc_output, 'r+')
    lines = tree_file.read()
    tree_file.close()
    tree_file = open(qmc_output, 'w')
    parsed = ""
    id_search = ""
    for character in lines:
        if(not character.isnumeric()):
            if(id_search == ""):
                parsed += character
            else:
                parsed += str(taxon_to_id[id_search])
                id_search = ""
                parsed+= character
        else:
            id_search+= character
    tree_file.write(parsed)
    tree_file.close()
    return


@parsl.python_app(executors=['single_thread'])
def setup_phylonet_data(basedir: str,
                        config: BioConfig,
                        inputs=[],
                        outputs=[],
                        stderr=parsl.AUTO_LOGNAME,
                        stdout=parsl.AUTO_LOGNAME):
    """Get the raxml/iqtree's output and create a NEXUS file as output for the phylonet in the basedir

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    if(config.tree_method == "ML_RAXML"):
        gene_trees = os.path.join(basedir, config.raxml_dir)
        gene_trees = os.path.join(gene_trees, config.raxml_output)
    elif(config.tree_method == "ML_IQTREE"):
        gene_trees = os.path.join(basedir, config.iqtree_dir)
        gene_trees = os.path.join(gene_trees, config.iqtree_output)
    out_dir = os.path.join(basedir, "phylonet")
    out_filepath = os.path.join(out_dir, config.phylonet_input)
    try:
        in_file = open(gene_trees)
    except IOError:
        print("Error! Could not open Gene tree file.")
        return 
    try:
        out_file = open(out_filepath)
    except IOError:
        print("Error! Could not open output file.")
        return
    tree_index = 0
    buffer = "#NEXUS\nBEGIN TREES;\n"
    for tree in in_file.readlines():
        tree_index+=1
        buffer+="Tree geneTree" + str(tree_index) + " = " + tree
    in_file.close()
    buffer+='END;\nBEGIN PHYLONET;\nInferNetwork_MP ('
    for i in range(0, tree_index-1):
        buffer+="geneTree" + str(i+1) +','
    output_network = os.path.join(out_dir, 'PhyloNet' + hmax + ".nex")
    buffer+="geneTree" + str(tree_index) +') ' + config.phylonet_hmax + " -pl " + config.phylonet_threads + " -x " + config.phylonet_threads + " " + output_network + ';\nEND;'
    #---
    out_file.write(buffer)
    out_file.close()
    return


@parsl.bash_app(executors=['phylogenetic_network'])
def phylonet(basedir: str,
             config: BioConfig,
             inputs=[],
             outputs=[],
             stderr=parsl.AUTO_LOGNAME,
             stdout=parsl.AUTO_LOGNAME):
    """Runs PhyloNet using as input the phylonet_input variable

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    exec_phylonet = config.phylonet
    import os
    output_dir = os.path.join(basedir, "phylonet")
    input_file = os.path.join(output_dir, config.phylonet_input)
    # Return to Parsl to be executed on the workflow
    return f'cd {output_dir};{exec_phylonet} {input_file}'


@parsl.bash_app(executors=['tree_and_statistics'])
def iqtree(basedir: str, config: BioConfig,
           input_file: str,
           inputs=[],
           stderr=parsl.AUTO_LOGNAME,
           stdout=parsl.AUTO_LOGNAME):
    """Runs IQ-TREE's executable using as input a sequence alignment in phylip format

    Parameters:
        basedir: current working directory
    Returns:
        returns an parsl's AppFuture

    TODO: Provide provenance.

    NB:
        Stdout and Stderr are defaulted to parsl.AUTO_LOGNAME, so the log will be automatically 
        named according to task id and saved under task_logs in the run directory.
    """
    import os
    iqtree_dir = os.path.join(basedir, config.iqtree_dir)
    flags = f"-T {config.iqtree_threads} {config.iqtree_exec_param} -s {input_file}"
    # Return to Parsl to be executed on the workflow
    return f"cd {iqtree_dir}; {config.iqtree} {flags}"


@parsl.python_app(executors=['single_thread'])
def clear_temporary_files(basedir: str,
                          config: BioConfig,
                          inputs=[],
                          outputs=[],
                          stderr=parsl.AUTO_LOGNAME,
                          stdout=parsl.AUTO_LOGNAME):    
    #TODO
    return


@parsl.python_app(executors=['single_thread'])
def create_folders(basedir: str,
                   config: BioConfig,
                   folders=[],
                   inputs=[],
                   outputs=[],
                   stderr=parsl.AUTO_LOGNAME,
                   stdout=parsl.AUTO_LOGNAME):
    import os
    from pathlib import Path 
    for folder in folders:
        full_path = os.path.join(basedir, folder)
        try:
            Path(full_path).mkdir(exist_ok=True)
        except Exception:
            print(f'Impossible to create {full_path} folder')
    return
