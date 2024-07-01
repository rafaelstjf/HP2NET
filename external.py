import parsl
import apps
import bioconfig
import logging
import argparse
from infra_manager import workflow_config, wait_for_all
from utils import CircularList
from iqtree_snaq import iqtree_snaq


@parsl.python_app(executors=['single_partition'])
def raxml_snaq_app(bio_config: bioconfig.BioConfig, basedir: dict, prep, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    from raxml_snaq import raxml_snaq
    r = raxml_snaq(bio_config, basedir, prep)
    return


@parsl.python_app(executors=['single_partition'])
def raxml_phylonet_app(bio_config: bioconfig.BioConfig, basedir: dict, prep, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    from raxml_phylonet import raxml_phylonet
    r = raxml_phylonet(bio_config, basedir, prep)
    return r


@parsl.python_app(executors=['single_partition'])
def iqtree_snaq_app(bio_config: bioconfig.BioConfig, basedir: dict, prep, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):

    r = iqtree_snaq(bio_config, basedir, prep)
    return r


@parsl.python_app(executors=['single_partition'])
def iqtree_phylonet_app(bio_config: bioconfig.BioConfig, basedir: dict, prep, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    from iqtree_phylonet import iqtree_phylonet
    r = iqtree_phylonet(bio_config, basedir, prep)
    return r


@parsl.python_app(executors=['single_partition'])
def mrbayes_snaq_app(bio_config: bioconfig.BioConfig, basedir: dict, prep, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
    from mrbayes_snaq import mrbayes_snaq
    r = mrbayes_snaq(bio_config, basedir, prep)
    return r


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
                r = raxml_snaq_app(bio_config, basedir, prep)
            elif (tree_method == 'IQTREE'):
                r = iqtree_snaq_app(bio_config, basedir, prep)
            elif (tree_method == 'MRBAYES'):
                r = mrbayes_snaq_app(bio_config, basedir, prep)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        elif (network_method == 'MP'):
            if (tree_method == 'RAXML'):
                r = raxml_phylonet_app(bio_config, basedir, prep)
            elif (tree_method == 'IQTREE'):
                r = iqtree_phylonet_app(bio_config, basedir, prep)
            else:
                logging.error(
                    f'Invalid parameter combination: {bio_config.network_method} and {bio_config.tree_method}')
        else:
            logging.error(
                f'Invalid network method: {bio_config.network_method}')
        if r is not None:
            results.append(r)
            # wait_for_all(r)
    if bio_config.plot_networks == True:
        plot = apps.plot_networks(bio_config, inputs=results)
        wait_for_all([plot])
    else:
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
