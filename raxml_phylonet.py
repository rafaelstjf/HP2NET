import math, os, parsl, glob, logging
import apps
from infra_manager import workflow_config
import bioconfig
from utils import *
def raxml_phylonet(bio_config, basedir, prepare_to_run):
    dkf_config = workflow_config(bio_config, level=2)
    dkf = parsl.load(dkf_config)
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
                            inputs=prepare_to_run,
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
    return result