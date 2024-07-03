import math, os, parsl, glob, logging, argparse, json
import apps
from infra_manager import workflow_config, wait_for_all
import bioconfig
from utils import *

RAXML_SNAQ = 0
RAXML_PHYLONET = 1
IQTREE_SNAQ = 2
IQTREE_PHYLONET = 3
MRBAYES_SNAQ = 4

def raxml_snaq(bio_config, basedir):
    
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    result = list()
    ret_tree = list()
    datalist = list()
    pool = CircularList(math.floor(
        max_workers/int(bio_config.raxml_threads)))
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "phylip")
    datalist = glob.glob(os.path.join(dir_, '*.phy'))
    for i, input_file in enumerate(datalist):
        ret = apps.raxml(basedir=basedir,
                        config=bio_config,
                        input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(basedir=basedir, config=bio_config, inputs=ret_tree)
    logging.info("Using the Maximum Pseudo Likelihood Method")
    ret_ast = apps.astral(basedir, config=bio_config, inputs=[ret_sad])
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.snaq_threads)))
    for h in bio_config.snaq_hmax:
        ret_snq = apps.snaq(basedir, config=bio_config, hmax=h, inputs=[
                            ret_ast], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_snq)
        result.append(ret_snq)
    wait_for_all(result)


def raxml_phylonet(bio_config, basedir):
    result = list()
    ret_tree = list()
    datalist = list()
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    pool = CircularList(math.floor(
        max_workers/int(bio_config.raxml_threads)))
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "phylip")
    datalist = glob.glob(os.path.join(dir_, '*.phy'))
    for i, input_file in enumerate(datalist):
        ret = apps.raxml(basedir=basedir,
                        config=bio_config,
                        input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(basedir=basedir, config=bio_config, inputs=ret_tree)
    ret_rooted = apps.root_tree(basedir, config=bio_config, inputs=[ret_sad])
    logging.info("Using the Maximum Parsimony Method")
    out_dir = os.path.join(basedir['dir'], bio_config.phylonet_dir)
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.phylonet_threads)))
    for h in bio_config.phylonet_hmax:
        ret_spd = apps.setup_phylonet_data(
            basedir, config=bio_config, hmax=h, inputs=[ret_rooted])
        filename = os.path.join(
            out_dir, (basedir['tree_method'] + '_' + h + '_' + bio_config.phylonet_input))
        ret_phylonet = apps.phylonet(basedir, config=bio_config, input_file=filename, inputs=[
                                     ret_spd], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_phylonet)
        result.append(ret_phylonet)
    wait_for_all(result)

def iqtree_snaq(bio_config, basedir):
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    result = list()
    ret_tree = list()
    datalist = list()
    pool = CircularList(math.floor(
        max_workers/int(bio_config.iqtree_threads)))
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "phylip")
    datalist = glob.glob(os.path.join(dir_, '*.phy'))
    for input_file in datalist:
        ret = apps.iqtree(basedir, bio_config,
                            input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
    logging.info("Using the Maximum Pseudo Likelihood Method")
    ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.snaq_threads)))
    for h in bio_config.snaq_hmax:
        ret_snq = apps.snaq(basedir, bio_config, h, inputs=[
                            ret_ast], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_snq)
        result.append(ret_snq)
    wait_for_all(result)

def iqtree_phylonet(bio_config, basedir):
    result = list()
    ret_tree = list()
    datalist = list()
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    pool = CircularList(math.floor(max_workers/int(bio_config.iqtree_threads)))
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "phylip")
    datalist = glob.glob(os.path.join(dir_, '*.phy'))
    for input_file in datalist:
        ret = apps.iqtree(basedir, bio_config,
                            input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
    ret_rooted = apps.root_tree(basedir, bio_config, inputs=[ret_sad])
    logging.info("Using the Maximum Parsimony Method")
    out_dir = os.path.join(basedir['dir'], bio_config.phylonet_dir)
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.phylonet_threads)))
    for h in bio_config.phylonet_hmax:
        ret_spd = apps.setup_phylonet_data(
            basedir, bio_config, h, inputs=[ret_rooted])
        filename = os.path.join(
            out_dir, (basedir['tree_method'] + '_' + h + '_' + bio_config.phylonet_input))
        ret_phylonet = apps.phylonet(basedir, bio_config, filename, inputs=[
                                     ret_spd], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_phylonet)
        result.append(ret_phylonet)

    #wait for all the 
    wait_for_all(result)


def mrbayes_snaq(bio_config, basedir):
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    result = list()
    ret_tree = list()
    datalist = list()
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "nexus")
    datalist = glob.glob(os.path.join(dir_, '*.nex'))
    ret_mbsum = list()
    for input_file in datalist:
        ret_mb = apps.mrbayes(basedir, bio_config,
                              input_file=input_file)
        ret_mbsum.append(apps.mbsum(basedir, bio_config,
                         input_file=input_file, inputs=[ret_mb]))
    ret_pre_bucky = apps.setup_bucky_data(
        basedir, bio_config, inputs=ret_mbsum)
    wait_for_all([ret_pre_bucky])
    bucky_folder = os.path.join(basedir['dir'], "bucky")
    prune_trees = glob.glob(os.path.join(bucky_folder, "*.txt"))
    ret_bucky = list()
    for prune_tree in prune_trees:
        ret_bucky.append(apps.bucky(basedir, bio_config,
                         prune_file=prune_tree, inputs=[ret_pre_bucky]))
    ret_post_bucky = apps.setup_bucky_output(
        basedir, bio_config, inputs=ret_bucky)
    ret_pre_qmc = apps.setup_qmc_data(
        basedir, bio_config, inputs=[ret_post_bucky])
    ret_qmc = apps.quartet_maxcut(basedir, bio_config, inputs=[ret_pre_qmc])
    ret_tree.append(apps.setup_qmc_output(
        basedir, bio_config, inputs=[ret_qmc]))
    logging.info("Using the Maximum Pseudo Likelihood Method")
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.snaq_threads)))
    for h in bio_config.snaq_hmax:
        ret_snq = apps.snaq(basedir, bio_config, h,
                            inputs=ret_tree, next_pipe=pool_phylo.next())
        pool_phylo.current(ret_snq)
        result.append(ret_snq)
    wait_for_all(result)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process the configuration file.')
    parser.add_argument('-s', '--settings', help='Settings file',
                        required=True, type=str)
    parser.add_argument('-d', '--basedir', required=True, type=str)
    parser.add_argument('-w', '--workflow', required=True, type=int)
    args = parser.parse_args()
    basedir = json.loads(args.basedir) # loads the dict that was previously dumped as json
    # load the settings
    config_file=args.settings
    cf = bioconfig.ConfigFactory(config_file)
    bio_config = cf.build_config()
    dkf_config = workflow_config(bio_config, level=2)
    dfk = parsl.load(dkf_config)
    # call the right pipeline
    workflow = args.pipeline
    if workflow == RAXML_SNAQ:
        raxml_snaq(bio_config = bio_config, basedir=basedir)
    elif workflow == RAXML_PHYLONET:
        raxml_phylonet(bio_config = bio_config, basedir=basedir)
    elif workflow == IQTREE_SNAQ:
        iqtree_snaq(bio_config = bio_config, basedir=basedir)
    elif workflow == IQTREE_PHYLONET:
        iqtree_phylonet(bio_config = bio_config, basedir=basedir)
    else:
        mrbayes_snaq(bio_config = bio_config, basedir=basedir)
