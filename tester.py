import parsl, apps, glob, bioconfig, os, logging, argparse, math, filecmp
from pandas.core import base
from datetime import datetime
from infra_manager import workflow_config, wait_for_all, CircularList
def test_raxml(bio_config, logger1 = None):
    logger1.critical("Testing RAXML...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"RAXML",
        "network_method":"MPL",
        "mapping":"",
        "outgroup": ""
    }
    seed = 123
    old_files = glob.glob(os.path.join(os.getcwd(), "tests/raxml/*.input"))
    for f in old_files:
        os.remove(f)
    input_file = os.path.join(os.getcwd(), "tests/raxml/input.phy")
    baseline_file = os.path.join(os.getcwd(), "tests/raxml/RAxML_bestTree.baseline")
    out_file = os.path.join(os.getcwd(), "tests/raxml/RAxML_bestTree.input")
    ret_tree = apps.raxml(basedir, bio_config, input_file=input_file, seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")

def test_iqtree(bio_config, logger1 = None):
    logger1.critical("Testing IQTREE...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"IQTREE",
        "network_method":"MPL",
         "mapping":"",
        "outgroup": ""
    }
    seed = 123
    input_file = os.path.join(os.getcwd(), "tests/iqtree/input.phy")
    baseline_file = os.path.join(os.getcwd(), "tests/iqtree/baseline.treefile")
    out_file = os.path.join(os.getcwd(), "tests/iqtree/input.phy.treefile")
    ret_tree = apps.iqtree(basedir, bio_config, input_file=input_file, seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")

def test_astral(bio_config, logger1 = None):
    logger1.critical("Testing ASTRAL...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"RAXML",
        "network_method":"MPL",
         "mapping":"",
        "outgroup": ""
    }
    seed = 123
    input_file = os.path.join(os.getcwd(), "tests/astral/BSlistfiles")
    baseline_file = os.path.join(os.getcwd(), "tests/astral/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/astral/besttree.tre")
    ret_tree = apps.astral(basedir, bio_config, inputs=[input_file], seed=seed)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")

def test_mbsum(bio_config, logger1 = None):
    logger1.critical("Testing MBSUM...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"MRBAYES",
        "network_method":"MPL",
         "mapping":"",
        "outgroup": ""
    }
    input_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex.run1.t")
    baseline_file = os.path.join(os.getcwd(), "tests/mbsum/baseline.sum")
    out_file = os.path.join(os.getcwd(), "tests/mbsum/input.nex.sum")
    ret_tree = apps.mbsum(basedir, bio_config, inputs=[input_file])
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")

def test_bucky(bio_config, logger1 = None):
    pass
def test_mrbayes(bio_config, logger1 = None):
    logger1.critical("Testing MrBayes...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"MRBAYES",
        "network_method":"MPL",
         "mapping":"",
        "outgroup": ""
    }
    input_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex")
    baseline_file = os.path.join(os.getcwd(), "tests/mrbayes/baseline.nex.run1.t")
    out_file = os.path.join(os.getcwd(), "tests/mrbayes/input.nex.run1.t")
    ret_tree = apps.quartet_maxcut(basedir, bio_config, inputs=input_file)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")

def test_phylonet(bio_config, logger1 = None):
    pass
def test_snaq(bio_config, logger1 = None):
    pass
def test_quartetmaxcut(bio_config, logger1 = None):
    logger1.critical("Testing Quartet Maxcut...")
    basedir = {
        "dir":os.path.join(os.getcwd(), "tests"),
        "tree_method":"RAXML",
        "network_method":"MPL",
         "mapping":"",
        "outgroup": ""
    }
    input_file = os.path.join(os.getcwd(), "tests/qmc/tests.txt")
    baseline_file = os.path.join(os.getcwd(), "tests/qmc/baseline.tre")
    out_file = os.path.join(os.getcwd(), "tests/qmc/tests.tre")
    ret_tree = apps.quartet_maxcut(basedir, bio_config, inputs=input_file)
    try:
        ret_tree.result()
        if(filecmp.cmp(baseline_file, out_file)):
            logger1.critical("\t...Passed!")
        else:
            logger1.critical("\t...Failed!")
    except Exception as e:
        logger1.critical("\t...Failed!")


def main(config_file='default.ini', workload_file = None):

    if workload_file is not None:
        cf = bioconfig.ConfigFactory(config_file, custom_workload = workload_file)
    else:
        cf = bioconfig.ConfigFactory(config_file)
    bio_config = cf.build_config()
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
    name = bio_config.workflow_name
    parsl.set_file_logger(f'{name}_script_{date_time}.output', level=logging.CRITICAL)
    parsl.set_stream_logger(level=logging.CRITICAL)
    logger1 = logging.getLogger("parsl")
    logger1.critical('Starting the Testing module')
    dkf_config = workflow_config(bio_config)
    dkf = parsl.load(dkf_config)
    test_raxml(bio_config, logger1)
    test_iqtree(bio_config, logger1)
    test_astral(bio_config, logger1)
    test_bucky(bio_config, logger1)
    test_quartetmaxcut(bio_config, logger1)
    test_mbsum(bio_config, logger1)
    test_mrbayes(bio_config, logger1)
    test_phylonet(bio_config, logger1)
    test_snaq(bio_config, logger1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the configuration file.')
    parser.add_argument('--cf', help='Settings file', required=False, type=str)
    args = parser.parse_args()
    if args.cf is not None:
        main(config_file=args.cf)
    else:
        main()
