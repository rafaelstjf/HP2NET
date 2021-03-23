import logging
import parsl


def main():
    import apps
    import glob
    from workflow import workflow_config, wait_for_all
    logging.info('Starting the Workflow Orchastration')

    # Configure the infrastructure
    # TODO: Fetch the configuration from a file...
    dkf_config = workflow_config(name='BioWorkFlow',
                                 monitor=False,
                                 partition=['sequana_cpu', 'sequana_cpu',
                                            'sequana_cpu', 'sequana_cpu_long'],
                                 walltime=['01:20:00', '01:20:00',
                                           '01:20:00', '01:20:00']
                                 )
    dkf = parsl.load(dkf_config)

    # Read where datasets are...
    work_list = list()
    with open('work.config', 'r') as reader:
        for line in reader.readlines():
            if line[0] == '#':
                continue
            baseline = line.strip()
            work_list.append(baseline)
            logging.info(f'Adding {baseline} to the list')

    result = list()
    for basedir in work_list:
        result.append(apps.setup_phylip_data(basedir))
    wait_for_all(result)

    result = list()
    for basedir in work_list:
        ret_rax = list()
        datalist = glob.glob(basedir + '/input/phylip/*')
        for input_file in datalist:
            ret_rax.append(apps.raxml(basedir, input_file))
        ret_sad = apps.setup_astral_data(basedir, inputs=ret_rax)
        ret_ast = apps.astral(basedir, inputs=[ret_sad])
        ret_snq = apps.snaq(basedir, inputs=[ret_ast])
        result.append(ret_snq)
    wait_for_all(result)

    return


# LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
