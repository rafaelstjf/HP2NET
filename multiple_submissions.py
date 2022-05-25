import time, os, glob, re, argparse, subprocess
from datetime import datetime
TIME_PATTERN = '%Y-%m-%d %H:%M:%S.%f' #parsl.log timestamp pattern
SLEEP_TIME = 60 #sleep for 50 min
NUMBER_EXEC = 3
def check_runing():
    try:
        status = subprocess.check_output("squeue -u rafael.terra | grep \" R \"", shell=True)
    except Exception:
        status = ""
    if(len(status) > 0):
        return True
    else:
        return False
def check_waiting():
    try:
        status = subprocess.check_output("squeue -u rafael.terra | grep \" PD \"", shell=True)
    except Exception:
        status = ""
    if(len(status) > 0):
        return True
    else:
        return False
times = list()
configs = ['100g6t_iqtree_phylonet.config', '100g6t_iqtree_snaq.config', '100g6t_raxml_phylonet.config', '100g6t_raxml_snaq.config', '100g6t_mrbayes_snaq.config', '100g6t_all.config']
#configs = ['300g6t_iqtree_phylonet.config', '300g6t_iqtree_snaq.config', '300g6t_raxml_phylonet.config', '300g6t_raxml_snaq.config', '300g6t_mrbayes_snaq.config', '300g6t_all.config']
#configs = ['1000g6t_iqtree_phylonet.config', '1000g6t_iqtree_snaq.config', '1000g6t_raxml_phylonet.config', '1000g6t_raxml_snaq.config', '1000g6t_mrbayes_snaq.config', '1000g6t_all.config']
#configs = ['1000g6t_all.config']
for i in range(0, NUMBER_EXEC*len(configs)):
    ind = i % len(configs)
    print(f'Submitting {configs[ind]}')
    while(check_runing()):
        print(f'There is a job running. Sleeping...')
        time.sleep(SLEEP_TIME)
    subprocess.call(f'sbatch --export=ALL,SCRATCH=\'{os.environ["SCRATCH"]}\',WF=\'{os.path.join(os.getcwd(), os.path.join("config",configs[ind]))}\' submit.sh', shell=True)
    while(check_runing() or check_waiting()):
        print(f'There is a job waiting or running. Sleeping...')
        time.sleep(SLEEP_TIME)
    files = sorted(glob.glob(os.path.join(os.getcwd(), 'runinfo/*')))
    last_runinfo = files[len(files) - 1]
    log = os.path.join(last_runinfo, 'parsl.log')
    first_line = ""
    last_line = ""
    with open(log, "rb") as file:
        try:
            file.seek(0)
            first_line =  file.readline().decode()
            file.seek(-2, os.SEEK_END)
            while file.read(1) != b'\n':
                file.seek(-2, os.SEEK_CUR)
        except OSError:
            file.seek(0)
        last_line = file.readline().decode()
    time_begin = re.match('\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', first_line).group(0)
    time_end = re.match('\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', last_line).group(0)
    total_time = datetime.strptime(time_end, TIME_PATTERN) - datetime.strptime(time_begin, TIME_PATTERN)
    with open("output.txt", 'a') as output:
        output.write(f'{configs[ind]},{total_time.total_seconds()}\n')
        output.close()

