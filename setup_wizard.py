import os, glob, configparser

class CustomError(Exception):
    pass

def load_default_config():
    config_dir = os.path.join(os.getcwd(), 'config')
    config_file = os.path.join(config_dir, 'default.ini')
    if os.path.isfile(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    else:
        raise CustomError(f"File {config_file} doesn't exist!")

def show_options(options_list):
    size = len(options_list)
    in_options = True
    if(size == 1 and options_list[0] ==''):
        return ""
    for i in range(1, size+1):
        print(f"\t{i} - {options_list[i-1]}")
    while in_options:
        option = input("\tType your option: ")
        if(option.isnumeric()):
            op = int(option)
            if(op >= 1 and op <= size):
                in_options = False
                return options_list[op-1]
        print("\tWrong Option! Try again.")
def show_and_add_options(options_list):
    size = len(options_list)
    in_options = True
    if(size == 1 and options_list[0] ==''):
        size = 0
    for i in range(1, size+1):
        print(f"\t{i} - {options_list[i-1]}")
    print(f'\t{size+1} - Custom parameter')
    while in_options:
        option = input("\tType your option: ")
        if(option.isnumeric()):
            op = int(option)
            if(op >= 1 and op <= size+1):
                in_options = False
                if(op == size+1):
                    return input("\tType your custom parameter: ")
                else:
                    return options_list[op-1]
        print("\tWrong Option! Try again.")

def main():
    print("----------------------HP2NETW configuration wizard----------------------")
    print("What do you want to do a full configuration?")
    full_configuration = show_options(['Yes', 'No'])
    cf = load_default_config()
    wf_dir = os.getcwd()
    config_dir = os.path.join(os.getcwd(), 'config')
    filename = input("Type the name of your configuration (.ini extension will be added automatically): ")
    config_file = os.path.join(config_dir, f'{filename}.ini')
    print("----------------------General settings----------------------")
    print('ExecutionProvider:')
    cf['GENERAL']['executionprovider'] = show_options(['SlurmProvider', 'LocalProvider'])
    cf['GENERAL']['sdScript'] = os.path.join(wf_dir, 'scripts')
    if(full_configuration == 'Yes' or cf['GENERAL']['executionprovider'] != 'LocalProvider'):
        print("Environment file:")
        cf['GENERAL']['environ'] = show_and_add_options([cf['GENERAL']['environ']])
    print("Workload file:")
    cf['GENERAL']['workload'] = show_and_add_options([cf['GENERAL']['workload']])
    print("Network Method:")
    cf['GENERAL']['networkmethod'] = show_options(['MP', 'MPL'])
    print("Tree Method:")
    if(cf['GENERAL']['networkmethod'] == 'MPL'):
        cf['GENERAL']['treemethod'] = show_options(['ML_RAXML', 'ML_IQTREE', 'BI_MRBAYES'])
    else:
        cf['GENERAL']['treemethod'] = show_options(['ML_RAXML', 'ML_IQTREE'])
    if(full_configuration == 'Yes' or cf['GENERAL']['executionprovider'] != 'LocalProvider'):
        print("----------------------Executor settings----------------------")
        print('Name of the fast partition:')
        cf['WORKFLOW']['partitionfast'] = show_and_add_options([cf['WORKFLOW']['partitionfast']])
        print("Number of cores of this partition: ")
        cf['WORKFLOW']['partcorefast'] = show_and_add_options([cf['WORKFLOW']['partcorefast']])
        print("Number of nodes of this partition: ")
        cf['WORKFLOW']['partnodefast'] = show_and_add_options([cf['WORKFLOW']['partnodefast']])
        print("Walltime this partition: ")
        cf['WORKFLOW']['walltimefast'] = show_and_add_options([cf['WORKFLOW']['walltimefast']])
        print('Name of the multithread partition:')
        cf['WORKFLOW']['partitionthread'] = show_and_add_options([cf['WORKFLOW']['partitionthread']])
        print("Number of cores of this partition: ")
        cf['WORKFLOW']['partcorethread'] = show_and_add_options([cf['WORKFLOW']['partcorethread']])
        print("Number of nodes of this partition: ")
        cf['WORKFLOW']['partnodethread'] = show_and_add_options([cf['WORKFLOW']['partnodethread']])
        print("Walltime this partition: ")
        cf['WORKFLOW']['walltimethread'] = show_and_add_options([cf['WORKFLOW']['walltimethread']])
        print('Name of the long partition:')
        cf['WORKFLOW']['partitionlong'] = show_and_add_options([cf['WORKFLOW']['partitionlong']])
        print("Number of cores of this partition: ")
        cf['WORKFLOW']['partcorelong'] = show_and_add_options([cf['WORKFLOW']['partcorelong']])
        print("Number of nodes of this partition: ")
        cf['WORKFLOW']['partnodelong'] = show_and_add_options([cf['WORKFLOW']['partnodelong']])
        print("Walltime this partition: ")
        cf['WORKFLOW']['walltimelong'] = show_and_add_options([cf['WORKFLOW']['walltimelong']])
    if(full_configuration == 'Yes' or cf['GENERAL']['treemethod'] == 'ML_RAXML'):
        print("----------------------RAxML settings----------------------")
        print("RAxML executable name")
        cf['RAXML']['raxmlexecutable'] = show_and_add_options([cf['RAXML']['raxmlexecutable']])
        print("Parameters:")
        cf['RAXML']['raxmlexecparameters'] = show_and_add_options([cf['RAXML']['raxmlexecparameters']])
        print("Number of threads:")
        cf['RAXML']['raxmlthreads'] = show_and_add_options([cf['RAXML']['raxmlthreads']])
    if(full_configuration == 'Yes' or cf['GENERAL']['treemethod'] == 'ML_IQTREE'):
        print("----------------------IQ-TREE settings----------------------")
        print("IQ-TREE executable name")
        cf['IQTREE']['iqtreeexecutable'] = show_and_add_options([cf['IQTREE']['iqtreeexecutable']])
        print("Parameters:")
        cf['IQTREE']['iqtreeparameters'] = show_and_add_options([cf['IQTREE']['iqtreeparameters']])
        print("Number of threads:")
        cf['IQTREE']['iqtreethreads'] = show_and_add_options([cf['IQTREE']['iqtreethreads']])
    if(full_configuration == 'Yes' or (cf['GENERAL']['networkmethod'] == 'MPL' and cf['GENERAL']['treemethod'] != 'BI_MRBAYES')):
        print("----------------------ASTRAL settings----------------------")
        print("Folder of the jar file:")
        cf['ASTRAL']['astralexecdir'] = show_and_add_options([cf['ASTRAL']['astralexecdir']])
        print("Name of the jar file:")
        cf['ASTRAL']['astraljar'] = show_and_add_options([cf['ASTRAL']['astraljar']])
        print("Parameters:")
        cf['ASTRAL']['astralexecparameters'] = show_and_add_options([cf['ASTRAL']['astralexecparameters']])
    if(full_configuration == 'Yes' or cf['GENERAL']['networkmethod'] == 'MPL' ):
        print("----------------------SNaQ (PhyloNetworks) settings----------------------")
        print("Script name (IMPORTANT: the script file has to be in the script directory):")
        cf['SNAQ']['snaqscript'] = show_and_add_options([cf['SNAQ']['snaqscript']])
        print("Number of threads:")
        cf['SNAQ']['snaqthreads'] = show_and_add_options([cf['SNAQ']['snaqthreads']])
        print("Maximum number of hybridizations:")
        cf['SNAQ']['snaqhmax'] = show_and_add_options([cf['SNAQ']['snaqhmax']])
        print("Number of runs:")
        cf['SNAQ']['snaqruns'] = show_and_add_options([cf['SNAQ']['snaqruns']])
    if(full_configuration == 'Yes' or cf['GENERAL']['networkmethod'] == 'MP' ):
        print("----------------------PhyloNet settings----------------------")
        print("Folder of the jar file:")
        cf['PHYLONET']['phylonetexecdir'] = show_and_add_options([cf['PHYLONET']['phylonetexecdir']])
        print("Name of the jar file:")
        cf['PHYLONET']['phylonetjar'] = show_and_add_options([cf['PHYLONET']['phylonetjar']])
        print("Number of threads:")
        cf['PHYLONET']['phylonetthreads'] = show_and_add_options([cf['PHYLONET']['phylonetthreads']])
        print("Maximum number of hybridizations:")
        cf['PHYLONET']['phylonethmax'] = show_and_add_options([cf['PHYLONET']['phylonethmax']])
    if(full_configuration == 'Yes' or cf['GENERAL']['treemethod'] == 'BI_MRBAYES' ):
        print("----------------------MrBayes settings----------------------")
        print("MrBayes executable name")
        cf['MRBAYES']['mbexecutable'] = show_and_add_options([cf['MRBAYES']['mbexecutable']])
        print("Parameters:")
        cf['MRBAYES']['mbparameters'] = show_and_add_options([cf['MRBAYES']['mbparameters']])
        print("----------------------BUCKy settings----------------------")
        print("BUCKy executable name")
        cf['BUCKY']['buckyexecutable'] = show_and_add_options([cf['BUCKY']['buckyexecutable']])
        print("MBSum executable name")
        cf['BUCKY']['mbsumexecutable'] = show_and_add_options([cf['BUCKY']['mbsumexecutable']])
        print("----------------------Quartet MaxCut settings----------------------")
        print("Executable folder:")
        cf['QUARTETMAXCUT']['qmcexecdir'] = show_and_add_options([cf['QUARTETMAXCUT']['qmcexecdir']])
        print("Quartet MaxCut executable name")
        cf['QUARTETMAXCUT']['qmcexecutable'] = show_and_add_options([cf['QUARTETMAXCUT']['qmcexecutable']])
    with open(config_file, 'w') as f:
        cf.write(f)
if __name__ == "__main__":
    main()