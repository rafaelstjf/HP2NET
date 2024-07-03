import parsl
import apps
import bioconfig
import logging
import argparse
import json
from infra_manager import workflow_config, wait_for_all
from utils import CircularList

RAXML_SNAQ = 0
RAXML_PHYLONET = 1
IQTREE_SNAQ = 2
IQTREE_PHYLONET = 3
MRBAYES_SNAQ = 4


@parsl.bash_app
def run_workflow(workflow: str, basedir: dict, config_filename: str,
                 stderr=parsl.AUTO_LOGNAME,
                 stdout=parsl.AUTO_LOGNAME):
    basedir_dict = json.dumps(basedir)
    return f"python3 isolated_pipelines.py --s {config_filename} --d {basedir_dict} --w {workflow}"


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
    prep.result()
    for basedir in bio_config.workload:
        r = None
        network_method = basedir['network_method']
        tree_method = basedir['tree_method']
        if (network_method == 'MPL'):
            if (tree_method == 'RAXML'):
                r = run_workflow(RAXML_SNAQ, basedir, config_file)
            elif (tree_method == 'IQTREE'):
                r = run_workflow(IQTREE_SNAQ, basedir, config_file)
            elif (tree_method == 'MRBAYES'):
                r = run_workflow(MRBAYES_SNAQ, basedir, config_file)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        elif (network_method == 'MP'):
            if (tree_method == 'RAXML'):
                r = run_workflow(RAXML_PHYLONET, basedir, config_file)
            elif (tree_method == 'IQTREE'):
                r = run_workflow(IQTREE_PHYLONET, basedir, config_file)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        else:
            logging.error(
                f'Invalid network method: {bio_config.network_method}')
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
