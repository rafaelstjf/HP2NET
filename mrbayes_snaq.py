import math, os, parsl, glob, logging
import apps
from infra_manager import workflow_config, wait_for_all
import bioconfig
from utils import *
def mrbayes_snaq(bio_config, basedir, prepare_to_run):
    dkf_config = workflow_config(bio_config, level=2)
    dkf = parsl.load(dkf_config)
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
                              input_file=input_file, inputs=prepare_to_run)
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
    return True