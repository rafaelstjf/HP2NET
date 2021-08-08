import parsl, apps, glob, bioconfig, os, logging, argparse
from pandas.core import base
from workflow import workflow_config, wait_for_all

def raxml_snaq(bio_config):
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = ['raxml', 'astral', 'snaq']
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        dir_ = os.path.join(os.path.join(basedir, "input"), "phylip")
        datalist = glob.glob(os.path.join(dir_, '*.phy'))
        #create trees       
        for input_file in datalist:
            ret = apps.raxml(basedir, bio_config, input_file)
            ret_tree.append(ret)
        wait_for_all(ret_tree)
        ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        logging.info("Using the Maximum Pseudo Likelihood Method")
        ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
        ret_snq = apps.snaq(basedir, bio_config, inputs=[ret_ast])
        #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_snq)
        result.append(ret_snq)
    wait_for_all(result)
    return

def raxml_phylonet(bio_config):
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = ['raxml', 'phylonet']
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        dir_ = os.path.join(os.path.join(basedir, "input"), "phylip")
        datalist = glob.glob(os.path.join(dir_, '*.phy'))
        #create trees
        for input_file in datalist:
            ret = apps.raxml(basedir, bio_config, input_file)
            ret_tree.append(ret)
        wait_for_all(ret_tree)
        ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        logging.info("Using the Maximum Parsimony Method")
        ret_spd = apps.setup_phylonet_data(basedir, bio_config, inputs=[ret_sad])
        ret_phylonet = apps.phylonet(basedir, bio_config, inputs=[ret_spd])
        #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_phylonet)
        result.append(ret_phylonet)
    wait_for_all(result)
    return

def iqtree_snaq(bio_config):
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = ['iqtree', 'astral', 'snaq']
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        dir_ = os.path.join(os.path.join(basedir, "input"), "phylip")
        datalist = glob.glob(os.path.join(dir_, '*.phy'))
        #create trees
        for input_file in datalist:
            ret  = apps.iqtree(basedir, bio_config, input_file)
            ret_tree.append(ret)
        wait_for_all(ret_tree)
        ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        logging.info("Using the Maximum Pseudo Likelihood Method")
        ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
        ret_snq = apps.snaq(basedir, bio_config, inputs=[ret_ast])
        #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_snq)
        result.append(ret_snq)
    wait_for_all(result)
    return

def iqtree_phylonet(bio_config):
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = ['iqtree', 'phylonet']
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        dir_ = os.path.join(os.path.join(basedir, "input"), "phylip")
        datalist = glob.glob(os.path.join(dir_, '*.phy'))
        #create trees
        for input_file in datalist:
            ret  = apps.iqtree(basedir, bio_config, input_file)
            ret_tree.append(ret)
        wait_for_all(ret_tree)
        ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        logging.info("Using the Maximum Parsimony Method")
        ret_spd = apps.setup_phylonet_data(basedir, bio_config, inputs=[ret_sad])
        ret_phylonet = apps.phylonet(basedir, bio_config, inputs=[ret_spd])
        #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_phylonet)
        result.append(ret_phylonet)
    wait_for_all(result)
    return

def mrbayes_snaq(bio_config):
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = ['mrbayes', 'bucky', 'mbsum', 'qmc', 'snaq']
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        dir_ = os.path.join(os.path.join(basedir, "input"), "nexus")
        datalist = glob.glob(os.path.join(dir_, '*.nex'))
        ret_mbsum = list()
        for input_file in datalist:
            ret_mb = apps.mrbayes(basedir, bio_config, input_file = input_file, inputs = [])
            ret_mbsum.append(apps.mbsum(basedir, bio_config, input_file = input_file, inputs = [ret_mb]))
        wait_for_all(ret_mbsum)
        ret_pre_bucky = apps.setup_bucky_data(basedir, bio_config, inputs = ret_mbsum)
        wait_for_all([ret_pre_bucky])
        bucky_folder = os.path.join(basedir, "bucky")	
        prune_trees = glob.glob(os.path.join(bucky_folder, "*.txt"))
        ret_bucky = list()
        for prune_tree in prune_trees:
            ret_bucky.append(apps.bucky(basedir, bio_config, prune_file = prune_tree, inputs = [ret_pre_bucky]))
        wait_for_all(ret_bucky)
        ret_post_bucky = apps.setup_bucky_output(basedir, bio_config, inputs = ret_bucky)
        ret_pre_qmc = apps.setup_qmc_data(basedir, bio_config, inputs = [ret_post_bucky])
        ret_qmc = apps.quartet_maxcut(basedir, bio_config, inputs = [ret_pre_qmc])
        ret_tree.append(apps.setup_qmc_output(basedir, bio_config, inputs = [ret_qmc]))
        wait_for_all(ret_tree)
        logging.info("Using the Maximum Pseudo Likelihood Method")
        ret_snq = apps.snaq(basedir, bio_config, inputs=ret_tree)
        result.append(ret_snq)
    wait_for_all(result)
    return

def main(config_file='config/default.ini', tree_method = "", network_method = ""):
    
    logging.info('Starting the Workflow Orchestration')

    cf = bioconfig.ConfigFactory(config_file)

    bio_config = cf.build_config()
    #configure parameters
    if tree_method == 'ML_RAXML' or tree_method == 'ML_IQTREE':
        bio_config.tree_method = tree_method
    if tree_method == 'BI_MRBAYES' and (bio_config.network_method == 'MPL' or network_method == 'MPL'):
        bio_config.tree_method = tree_method
    if network_method == 'MPL':
        bio_config.network_method = network_method
    elif network_method == 'MP' and bio_config.tree_method != 'BI_MRBAYES':
        bio_config.network_method = network_method
    # Configure the infrastructure
    # TODO: Fetch the configuration from a file...
    dkf_config = workflow_config(bio_config)
    dkf = parsl.load(dkf_config)
    if(bio_config.network_method == 'MPL'):
        if(bio_config.tree_method == 'ML_RAXML'):
            raxml_snaq(bio_config)
        elif(bio_config.tree_method == 'ML_IQTREE'):
            iqtree_snaq(bio_config)
        elif(bio_config.tree_method == 'BI_MRBAYES'):
            mrbayes_snaq(bio_config)
        else:
            logging.error(f'Invalid parameter combinatiion: {bio_config.network_method} and {bio_config.tree_method}')
    elif(bio_config.network_method == 'MP'):
        if(bio_config.tree_method == 'ML_RAXML'):
            raxml_phylonet(bio_config)
        elif(bio_config.tree_method == 'ML_IQTREE'):
            iqtree_phylonet(bio_config)
        else:
            logging.error(f'Invalid parameter combinatiion: {bio_config.network_method} and {bio_config.tree_method}')
    else:
        logging.error(f'Invalid network method: {bio_config.network_method}')
    return

# LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the configuration file.')
    parser.add_argument('--config', help='Settings file', required=False, type=str)
    parser.add_argument('--net', help='Network method (MP for maximum parsimony or MPL for maximum pseudo likelihood)', required=False, type=str)
    parser.add_argument('--tree', help='Tree method (ML_RAXML, ML_IQTREE or BI_MRBAYES (works only with MPL))', required=False, type=str)
    args = parser.parse_args()
    if args.config is not None and args.tree is not None and args.net is not None:
        main(config_file=args.config, tree_method = args.tree, network_method = args.net)
    elif args.config is not None and args.tree is not None:
        main(config_file=args.config, tree_method=args.tree)
    elif args.config is not None and args.net is not None:
        main(config_file=args.config, network_method=args.net)
    elif args.tree is not None and args.net is not None:
        main(tree_method = args.tree, network_method=args.net)
    elif args.config is not None:
        main(config_file=args.config)
    elif args.tree is not None:
        main(tree_method=args.tree)
    elif args.net is not None:
        main(network_method = args.net)
    else:
        main()