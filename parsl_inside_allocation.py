import logging
from os import curdir

### LOGGING SECTION
logging.basicConfig(level=logging.INFO)

     

if __name__ == "__main__":
    import os
    from workflow import loop_on_baseline_raxml, wait_for_all, astral
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
        rf = loop_on_baseline_raxml(basedir)
        #TODO: move the wait section to the end
        for i in rf.result():
            result.append(i)

    logging.info(f'Entering in wait_for_all raxml')
    wait_for_all(result)

    logging.info(f'Running Astral')
    result = list()
    for basedir in work_list:
        raxml_dir = f'{basedir}/raxml'
        os.system(f'rm {raxml_dir}/RAxML_info.*')
        try:
            os.mkdir(f'{raxml_dir}/bootstrap')
        except FileExistsError:
            os.system(f'rm -f {raxml_dir}/bootstrap/*')

        os.system(f'mv {raxml_dir}/RAxML_bootstrap.* {raxml_dir}/bootstrap')
        os.system(f'tar -czf {raxml_dir}/contrees.tgz {raxml_dir}/RAxML_bipartitions*')
        os.system(f'rm -f {raxml_dir}/RAxML_bipartitions*')
        os.system(f'cat {raxml_dir}/RAxML_bestTree.* > {raxml_dir}/besttrees.tre')
        os.system(f'tar -czf {raxml_dir}/besttrees.tgz {raxml_dir}/RAxML_bestTree.*')
        os.system(f'rm -f {raxml_dir}/RAxML_bestTree.*')
        r = astral(basedir)
        result.append(r)
        
    logging.info(f'Entering in wait_for_all atral')
    wait_for_all(result)
