import parsl
import apps
import glob
import bioconfig
import os
import logging
import argparse
import math
from infra_manager import workflow_config, wait_for_all
from utils import CircularList

reuse = False
cache = dict()
# LOGGING SECTION
logger = logging.getLogger()
logging.basicConfig(level=logging.CRITICAL)


def raxml_snaq(bio_config, basedir, prepare_to_run):
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
                         inputs=prepare_to_run,
                         input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(
        basedir=basedir, config=bio_config, inputs=ret_tree)
    logging.info("Using the Maximum Pseudo Likelihood Method")
    ret_ast = apps.astral(basedir, config=bio_config, inputs=[ret_sad])
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.snaq_threads)))
    for h in bio_config.snaq_hmax:
        ret_snq = apps.snaq(basedir, config=bio_config, hmax=h, inputs=[
                            ret_ast], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_snq)
        result.append(ret_snq)
    return result


def raxml_phylonet(bio_config, basedir, prepare_to_run):
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
    ret_sad = apps.setup_tree_output(
        basedir=basedir, config=bio_config, inputs=ret_tree)
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


def iqtree_snaq(bio_config, basedir, prepare_to_run):
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
        ret = apps.iqtree(basedir=basedir, config=bio_config,
                          inputs=prepare_to_run,
                          input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(
        basedir=basedir, config=bio_config, inputs=ret_tree)
    logging.info("Using the Maximum Pseudo Likelihood Method")
    ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
    pool_phylo = CircularList(math.floor(
        max_workers/int(bio_config.snaq_threads)))
    for h in bio_config.snaq_hmax:
        ret_snq = apps.snaq(basedir, bio_config, h, inputs=[
                            ret_ast], next_pipe=pool_phylo.next())
        pool_phylo.current(ret_snq)
        result.append(ret_snq)
    return result


def iqtree_phylonet(bio_config, basedir, prepare_to_run):
    result = list()
    ret_tree = list()
    datalist = list()
    max_workers = bio_config.workflow_core*bio_config.workflow_node
    pool = CircularList(math.floor(max_workers/int(bio_config.iqtree_threads)))
    # append the input files
    dir_ = os.path.join(os.path.join(basedir['dir'], "input"), "phylip")
    datalist = glob.glob(os.path.join(dir_, '*.phy'))
    for input_file in datalist:
        ret = apps.iqtree(basedir=basedir, config=bio_config, inputs=prepare_to_run,
                          input_file=input_file, next_pipe=pool.next())
        pool.current(ret)
        ret_tree.append(ret)
    ret_sad = apps.setup_tree_output(
        basedir=basedir, config=bio_config, inputs=ret_tree)
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
    return result


def mrbayes_snaq(bio_config, basedir, prepare_to_run):
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
    return result

def prepare_to_run(config):
    folder_list = list()
    r = list()
    phylip_folders = list()
    for basedir in config.workload:
        if basedir['dir'] not in phylip_folders:
            r.append(apps.setup_phylip_data(basedir, config))
            phylip_folders.append(basedir['dir'])
        network_method = basedir['network_method']
        tree_method = basedir['tree_method']
        if (network_method == 'MPL'):
            if (tree_method == 'RAXML'):
                folder_list.extend(
                    [config.raxml_dir, config.astral_dir, config.snaq_dir])
            elif (tree_method == 'IQTREE'):
                folder_list.extend(
                    [config.iqtree_dir, config.astral_dir, config.snaq_dir])
            elif (tree_method == 'MRBAYES'):
                folder_list.extend([config.mrbayes_dir, config.bucky_dir,
                                   config.mbsum_dir, config.quartet_maxcut_dir, config.snaq_dir])
        elif (network_method == 'MP'):
            if (tree_method == 'RAXML'):
                folder_list.extend([config.raxml_dir, config.phylonet_dir])
            elif (tree_method == 'IQTREE'):
                folder_list.extend([config.iqtree_dir, config.phylonet_dir])
        r.append(apps.create_folders(basedir, config, folders=folder_list))
    return r


def main(**kwargs):
    logging.info('Starting the Workflow Orchestration')
    if kwargs["config_file"] is not None:
        config_file = kwargs["config_file"]
    else:
        config_file = 'default.ini'
    if kwargs["workload_file"] is not None:
        cf = bioconfig.ConfigFactory(
            config_file, custom_workload=kwargs["workload_file"])
    else:
        cf = bioconfig.ConfigFactory(config_file)
    bio_config = cf.build_config()
    dkf_config = workflow_config(bio_config, **kwargs)
    logging.info(f"{hash(bio_config)}")
    dkf = parsl.load(dkf_config)
    results = list()
    prep = prepare_to_run(bio_config)
    for basedir in bio_config.workload:
        r = None
        network_method = basedir['network_method']
        tree_method = basedir['tree_method']
        if (network_method == 'MPL'):
            if (tree_method == 'RAXML'):
                r = raxml_snaq(bio_config, basedir, prep)
            elif (tree_method == 'IQTREE'):
                r = iqtree_snaq(bio_config, basedir, prep)
            elif (tree_method == 'MRBAYES'):
                r = mrbayes_snaq(bio_config, basedir, prep)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        elif (network_method == 'MP'):
            if (tree_method == 'RAXML'):
                r = raxml_phylonet(bio_config, basedir, prep)
            elif (tree_method == 'IQTREE'):
                r = iqtree_phylonet(bio_config, basedir, prep)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        else:
            logging.error(
                f'Invalid network method: {bio_config.network_method}')
        if r is not None:
            results.extend(r)
            # wait_for_all(r)
    if bio_config.plot_networks == True:
        plot = apps.plot_networks(bio_config, inputs=results)
        wait_for_all([plot])
    else:
        wait_for_all(results)
    parsl.dfk().cleanup() 
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process the configuration file.')
    parser.add_argument('-s', '--settings', help='Settings file',
                        required=False, type=str, default=None)
    parser.add_argument('-w', '--workload', help='Workload file',
                        required=False, type=str, default=None)
    parser.add_argument(
        "-r", "--runinfo", help="Folder to store the Parsl logs", required=False, type=str, default=None)
    parser.add_argument(
        '-m', "--maxworkers", help="Max workers", required=False, type=int, default=None)

    args = parser.parse_args()

    main(config_file=args.settings, workload_file=args.workload,
         max_workers=args.maxworkers, runinfo=args.runinfo)
