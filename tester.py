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
    old_files = glob.glob(os.path.join(os.getcwd(), "tests/raxml/*.output"))
    os.system(f"rm {old_files}")
    input_file = os.path.join(os.getcwd(), "tests/raxml/input.phy")
    baseline_file = os.path.join(os.getcwd(), "tests/raxml/RAxML_bestTree.baseline")
    out_file = os.path.join(os.getcwd(), "tests/raxml/RAxML_bestTree.output")
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
    baseline_file = os.path.join(os.getcwd(), "tests/iqtree/baseline.treefile")
    out_file = os.path.join(os.getcwd(), "tests/iqtree/input.treefile")
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

def test_mbsum(bio_config):
    print("Testing MBSUM...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"MRBAYES",
        "network_method":"MPL",
        "mapping":""
    }
    input_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex.run1.t")
    baseline_file = os.path.join(os.getcwd(), "tests/mbsum/baseline.sum")
    out_file = os.path.join(os.getcwd(), "tests/mbsum/input.nex.sum")
    ret_tree = apps.quartet_maxcut(basedir, bio_config, inputs=input_file)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            print("\t...Passed!")
        else:
            print("\t...Failed!")
    except Exception as e:
        print("\t...Failed!")

def test_bucky(bio_config):
    pass
def test_mrbayes(bio_config):
    print("Testing MrBayes...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"MRBAYES",
        "network_method":"MPL",
        "mapping":""
    }
    input_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex")
    baseline_file = os.path.join(os.getcwd(), "tests/mrbayes/baseline.nex.run1.t")
    out_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex.run1.t")
    ret_tree = apps.quartet_maxcut(basedir, bio_config, inputs=input_file)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            print("\t...Passed!")
        else:
            print("\t...Failed!")
    except Exception as e:
        print("\t...Failed!")

def test_phylonet(bio_config):
    pass
def test_snaq(bio_config):
    pass
def test_quartetmaxcut(bio_config):
    print("Testing Quartet Maxcut...")
    basedir = {
        "dir":os.getcwd(),
        "tree_method":"RAXML",
        "network_method":"MPL",
        "mapping":""
    }
    input_file = os.path.join(os.getcwd(), "tests/qmc/tests.txt")
    baseline_file = os.path.join(os.getcwd(), "tests/qmc/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/qmc/tests.tre")
    ret_tree = apps.quartet_maxcut(basedir, bio_config, inputs=input_file)
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
    test_bucky()
    test_quartetmaxcut()
    test_mbsum()
    test_mrbayes()
    test_phylonet()
    test_snaq()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the configuration file.')
    parser.add_argument('--cf', help='Settings file', required=False, type=str)
    args = parser.parse_args()
    if args.cf is not None:
        main(config_file=args.cf)
    else:
        main()
