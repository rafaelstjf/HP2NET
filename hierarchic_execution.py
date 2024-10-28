import parsl
import apps
import bioconfig
import logging
import argparse
import json
import math
from infra_manager import workflow_config, wait_for_all
from utils import CircularList

RAXML_SNAQ = 0
RAXML_PHYLONET = 1
IQTREE_SNAQ = 2
IQTREE_PHYLONET = 3
MRBAYES_SNAQ = 4
RAXML_SNAQ_PHYLONET = 5
IQTREE_SNAQ_PHYLONET = 6


@parsl.bash_app
def run_workflow(workflow: str, basedir: dict, config_filename: str, custom_workload: str,
                 stderr=parsl.AUTO_LOGNAME,
                 stdout=parsl.AUTO_LOGNAME):
    basedir_dict = json.dumps(basedir)
    return f"python3 isolated_pipelines.py -s {config_filename} -d \'{basedir_dict}\' -w \'{custom_workload}\' -p {workflow}"


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
    custom_workload = ""
    if kwargs["config_file"] is not None:
        config_file = kwargs["config_file"]
    else:
        config_file = 'default.ini'
    if kwargs["workload_file"] is not None:
        custom_workload = kwargs["workload_file"]
        cf = bioconfig.ConfigFactory(
            config_file, custom_workload=kwargs["workload_file"])
    else:
        cf = bioconfig.ConfigFactory(config_file)
    bio_config = cf.build_config()
    # distribute the workload between the nodes
    max_workers = math.ceil(len(bio_config.workload)/bio_config.workflow_node)
    dkf_config = workflow_config(bio_config, max_workers=max_workers)
    logging.info(f"{hash(bio_config)}")
    dkf = parsl.load(dkf_config)
    results = list()
    prep = prepare_to_run(bio_config)
    wait_for_all(prep)
    # Check in which directory there is reuse
    wfs_to_run = dict()
    for basedir in bio_config.workload:
        c_dir = basedir["dir"]
        network_method = basedir['network_method']
        tree_method = basedir['tree_method']
        if wfs_to_run.get(c_dir) is None:
            wfs_to_run[c_dir] = list()
        if (network_method == 'MPL'):
            if (tree_method == 'RAXML'):
                wfs_to_run[c_dir].append(RAXML_SNAQ)
            elif (tree_method == 'IQTREE'):
                wfs_to_run[c_dir].append(IQTREE_SNAQ)
            elif (tree_method == 'MRBAYES'):
                wfs_to_run[c_dir].append(MRBAYES_SNAQ)
        elif (network_method == 'MP'):
            if (tree_method == 'RAXML'):
                wfs_to_run[c_dir].append(RAXML_PHYLONET)
            elif (tree_method == 'IQTREE'):
                wfs_to_run[c_dir].append(IQTREE_PHYLONET)
    # Change the cod of the workflow for the reuse code
    for w in wfs_to_run.keys():
        if (RAXML_PHYLONET in wfs_to_run[w]) and (RAXML_SNAQ in wfs_to_run[w]):
            wfs_to_run[w].remove(RAXML_PHYLONET)
            wfs_to_run[w].remove(RAXML_SNAQ)
            wfs_to_run[w].append(RAXML_SNAQ_PHYLONET)
        
        if (IQTREE_PHYLONET in wfs_to_run[w]) and (IQTREE_SNAQ in wfs_to_run[w]):
            wfs_to_run[w].remove(IQTREE_PHYLONET)
            wfs_to_run[w].remove(IQTREE_SNAQ)
            wfs_to_run[w].append(IQTREE_SNAQ_PHYLONET)

    # modify the basedir according to the reuse list
    processed_dir = list()    
    for basedir in bio_config.workload:
        r = None
        if basedir["dir"] in processed_dir:
            continue
        processed_dir.append(basedir["dir"])
        c_basedir = basedir
        for w in wfs_to_run[basedir["dir"]]:
            if w == RAXML_SNAQ:
                c_basedir['tree_method'] = "RAXML"
                c_basedir['network_method'] = "MPL"
            elif w == RAXML_PHYLONET:
                c_basedir['tree_method'] = "RAXML"
                c_basedir['network_method'] = "MP"
            elif w == RAXML_SNAQ_PHYLONET:
                c_basedir['tree_method'] = "RAXML"
                c_basedir['network_method'] = "BOTH"
            elif w == IQTREE_SNAQ:
                c_basedir['tree_method'] = "IQTREE"
                c_basedir['network_method'] = "MPL"
            elif w == IQTREE_PHYLONET:
                c_basedir['tree_method'] = "IQTREE"
                c_basedir['network_method'] = "MP"
            elif w == IQTREE_SNAQ_PHYLONET:
                c_basedir['tree_method'] = "IQTREE"
                c_basedir['network_method'] = "BOTH"
            elif w == MRBAYES_SNAQ:
                c_basedir['tree_method'] = "MRBAYES"
                c_basedir['network_method'] = "MPL"
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
            r = run_workflow(w, c_basedir, config_file, custom_workload)
            if r is not None:
                results.append(r)
            # wait_for_all(r)
    wait_for_all(results)
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
    parsl.dfk().cleanup()
