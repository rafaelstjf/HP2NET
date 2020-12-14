import logging


def loop_on_baseline_raxml(basedir):
    import glob
    from workflow import raxml

    datalist = glob.glob(basedir + '/input/phylip/*')
    result = list()
    for input_file in datalist:
        fut_result = raxml(basedir, inputs=[input_file])
        result.append(fut_result)

    return result
   

### LOGGING SECTION
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    import os
    from workflow import workflow_config, loop_on_baseline_raxml, wait_for_all, astral, setup_phylip_data, setup_astral_data
    logging.info('Starting the Workflow Orchastration') 
    
    #Configure the infrastructure
    #TODO: Fetch the configuration from a file...
    workflow_config(name='BioWorkFlow',
                    nodes=4,
                    cores_per_node=8,
                    interval=1,
                    monitor=False)

    #Read where datasets are...
    work_list = list()
    with open('work.config', 'r') as reader:
        for line in reader.readlines():
            baseline = line.strip()
            work_list.append(baseline)
            logging.info(f'Adding {baseline} to the list')


    result = list()
    for basedir in work_list:
        ret_val = setup_phylip_data(basedir)
        result.append(ret_val)
    logging.info(f'Waiting for environment setup')
    wait_for_all(result)


    result = list()
    for basedir in work_list:
        rf = loop_on_baseline_raxml(basedir)
        #TODO: move the wait section to the end
        for i in rf:
            result.append(i)
    logging.info(f'Entering in wait_for_all raxml')
    wait_for_all(result)

    result = list()
    for basedir in work_list:
        ret_val = setup_astral_data(basedir)
        result.append(ret_val)
    logging.info(f'Entering in wait_for_all setup_astral_data')
    wait_for_all(result)

    
    result = list()
    for basedir in work_list:
        r = astral(basedir)
        result.append(r)        
    logging.info(f'Entering in wait_for_all astral')
    wait_for_all(result)
