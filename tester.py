import parsl, apps, glob, bioconfig, os, logging, argparse, math, filecmp
from pandas.core import base
from infra_manager import workflow_config, wait_for_all, CircularList

cache = dict()
#LOGGING SECTION
logging.basicConfig(level=logging.DEBUG)

def test_raxml(bio_config):
    print("Testing RAXML...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"RAXML",
        "network_method":"MPL",
        "mapping":""
    }
    seed = 123
    input_file = os.path.join(os.getcwd(), "tests/raxml/input.phy")
    baseline_file = os.path.join(os.getcwd(), "tests/raxml/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/raxml/besttree.tre")
    ret_tree = apps.raxml(basedir, bio_config, input_file=input_file, seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            print("\t...Passed!")
        else:
            print("\t...Failed!")
    except Exception as e:
        print("\t...Failed!")

def test_iqtree(bio_config):
    print("Testing IQTREE...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"IQTREE",
        "network_method":"MPL",
        "mapping":""
    }
    seed = 123
    input_file = os.path.join(os.getcwd(), "tests/iqtree/input.phy")
    baseline_file = os.path.join(os.getcwd(), "tests/iqtree/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/iqtree/besttree.tre")
    ret_tree = apps.iqtree(basedir, bio_config, input_file=input_file, seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            print("\t...Passed!")
        else:
            print("\t...Failed!")
    except Exception as e:
        print("\t...Failed!")

def test_astral(bio_config):
    print("Testing ASTRAL...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"RAXML",
        "network_method":"MPL",
        "mapping":""
    }
    seed = 123
    input_file = os.path.join(os.getcwd(), "tests/astral/BSlistfiles")
    baseline_file = os.path.join(os.getcwd(), "tests/astral/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/astral/besttree.tre")
    ret_tree = apps.astral(basedir, bio_config, inputs=input_file, seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            print("\t...Passed!")
        else:
            print("\t...Failed!")
    except Exception as e:
        print("\t...Failed!")

def main(config_file='default.ini', workload_file = None):
    logging.info('Starting the Testing module')
    if workload_file is not None:
        cf = bioconfig.ConfigFactory(config_file, custom_workload = workload_file)
    else:
        cf = bioconfig.ConfigFactory(config_file)
    bio_config = cf.build_config()
    dkf_config = workflow_config(bio_config)
    dkf = parsl.load(dkf_config)
    test_raxml()
    test_iqtree()
    test_astral()
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the configuration file.')
    parser.add_argument('--cf', help='Settings file', required=False, type=str)
    parser.add_argument('--wf', help='Workload file', required=False, type=str)
    args = parser.parse_args()
    if args.cf is not None:
        if args.wf is not None:
            main(config_file=args.cf, workload_file=args.wf)
        else:
            main(config_file=args.cf)
    else:
        if args.wf is not None:
            main(workload_file=args.wf)
        else:
            main()
