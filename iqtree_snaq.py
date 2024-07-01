import math, os, parsl, glob, logging
import apps
from infra_manager import workflow_config, wait_for_all
import bioconfig
from utils import *

def iqtree_snaq(bio_config, basedir, prepare_to_run):
    dkf_config = workflow_config(bio_config, level=2)
    dkf = parsl.load(dkf_config)
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
                            inputs=prepare_to_run,
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
    f = 1
    return f
