import logging

from pandas.core import base
import parsl

def main():
    import apps
    import glob
    import bioconfig
    import os
    from workflow import workflow_config, wait_for_all
    logging.info('Starting the Workflow Orchestration')

    cf = bioconfig.ConfigFactory()

    bio_config = cf.build_config()

    # Configure the infrastructure
    # TODO: Fetch the configuration from a file...
    dkf_config = workflow_config(bio_config)
    dkf = parsl.load(dkf_config)

    # Read where datasets are
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        #Create folders
        folder_list = []
        if(bio_config.tree_method == 'ML_RAXML'):
            folder_list.append('raxml')
        elif(bio_config.tree_method == 'ML_IQTREE'):
            folder_list.append('iqtree')
        elif(bio_config.tree_method == "BI_MRBAYES"):
            folder_list.extend(['mrbayes', 'bucky', 'mbsum', 'qmc'])
        if(bio_config.network_method == "MPL"):
            if(bio_config.tree_method != "BI_MRBAYES"):
                folder_list.append('astral')
            folder_list.append('snaq')
        result.append(apps.setup_phylip_data(basedir, bio_config))
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))
    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = list()
        #append the input files
        if(bio_config.tree_method == 'BI_MRBAYES'):
            dir_ = os.path.join(basedir, "input")
            dir_ = os.path.join(dir_, "nexus")
            datalist = glob.glob(os.path.join(dir_, '*.nex'))
        else:
            dir_ = os.path.join(basedir, "input")
            dir_ = os.path.join(dir_, "phylip")
            datalist = glob.glob(os.path.join(dir_, '*.phy'))
        #create trees
        if(bio_config.tree_method == 'ML_RAXML'):            
            for input_file in datalist:
                ret = apps.raxml(basedir, bio_config, input_file)
                ret_tree.append(ret)
            wait_for_all(ret_tree)
        elif(bio_config.tree_method == 'ML_IQTREE'):
            for input_file in datalist:
                ret  = apps.iqtree(basedir, bio_config, input_file)
                ret_tree.append(ret)
            wait_for_all(ret_tree)
        if(bio_config.tree_method == "BI_MRBAYES"):
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
        else:
            ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        if(bio_config.network_method == "MPL"):
            logging.info("Using the Maximum Pseudo Likelihood Method")
            if(bio_config.tree_method == "BI_MRBAYES"):
                ret_snq = apps.snaq(basedir, bio_config, inputs=ret_tree)
            else:
                ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
                ret_snq = apps.snaq(basedir, bio_config, inputs=[ret_ast])
                #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_snq)
            result.append(ret_snq)
        elif(bio_config.network_method == "MP" and bio_config.tree_method != "BI_MRBAYES"):
            logging.info("Using the Maximum Parsimony Method")
            ret_spd = apps.setup_phylonet_data(basedir, bio_config, inputs=ret_tree)
            ret_phylonet = apps.phylonet(basedir, bio_config, inputs=[ret_spd])
            #ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_phylonet)
            result.append(ret_phylonet)
    wait_for_all(result)
    return


# LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
