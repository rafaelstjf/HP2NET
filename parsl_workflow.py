import logging
import parsl


def main():
    import apps
    import glob
    import bioconfig
    from workflow import workflow_config, wait_for_all
    logging.info('Starting the Workflow Orchastration')

    cf = bioconfig.ConfigFactory()

    bio_config = cf.build_config()

    # Configure the infrastructure
    # TODO: Fetch the configuration from a file...
    dkf_config = workflow_config(bio_config)
    dkf = parsl.load(dkf_config)

    # Read where datasets are...
    work_list = bio_config.workload
    result = list()
    for basedir in work_list:
        result.append(apps.setup_phylip_data(basedir, bio_config))
        folder_list = []
        if(bio_config.tree_method == 'ML-RAXML'):
            folder_list.append('raxml')
        elif(bio_config.tree_method == 'ML-IQTREE'):
            folder_list.append('iqtree')
        if(bio_config.network_method == "MPL"):
            folder_list.append('astral')
        result.append(apps.create_folders(basedir, bio_config,folders=folder_list))

    wait_for_all(result)
    result = list()
    for basedir in work_list:
        ret_tree = list()
        datalist = glob.glob(basedir + '/input/phylip/*.phy')
        if(bio_config.tree_method == 'ML-RAXML'):            
            for input_file in datalist:
                ret = apps.raxml(basedir, bio_config, input_file)
                ret_tree.append()
        elif(bio_config.tree_method == 'ML-IQTREE'):
            for input_file in datalist:
                ret  = apps.iqtree(basedir, bio_config, input_file)
                ret_tree.append()
        ret_sad = apps.setup_tree_output(basedir, bio_config, inputs=ret_tree)
        if(bio_config.network_method == "MPL"):
            logging.info("Using the Maximum Pseudo Likelihood Method")
            ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
            ret_snq = apps.snaq(basedir, bio_config, inputs=[ret_ast])
            ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_snq)
            result.append(ret_clear)
        elif(bio_config.network_method == "MP"):
            logging.info("Using the Maximum Parsimony Method")
            ret_spd = apps.setup_phylonet_data(basedir, bio_config, inputs=ret_tree)
            ret_phylonet = apps.phylonet(basedir, bio_config, inputs=[ret_spd])
            ret_clear = apps.clear_temporary_files(basedir, bio_config, inputs=ret_phylonet)
            result.append(ret_clear)
    wait_for_all(result)


    return


# LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
