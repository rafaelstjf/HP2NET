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
    wait_for_all(result)

    result = list()
    for basedir in work_list:
        ret_rax = list()
        datalist = glob.glob(basedir + '/input/phylip/*')
        for input_file in datalist:
            ret_rax.append(apps.raxml(basedir, bio_config, input_file))
        ret_sad = apps.setup_astral_data(basedir, bio_config, inputs=ret_rax)
        ret_ast = apps.astral(basedir, bio_config, inputs=[ret_sad])
        ret_snq = apps.snaq(basedir, bio_config, inputs=[ret_ast])
        result.append(ret_snq)
    wait_for_all(result)

    return


# LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
