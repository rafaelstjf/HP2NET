# Configuration file


def getconfig(name: str):
	"""Get the current configuration.

	TODO: write a proper function and interface with a database.
	"""
	return { 	 
			"astraljar": 	 "/Users/carvalho/tmp/bio/Astral/astral.5.7.4.jar",
			"raxmlscripts":		"/Users/carvalho/tmp/bio/data_results/scripts",
			"seqdir":			"input/nexus",
			"raxmldir":			"raxml",
			"astraldir":		"astral",
			"datadir":			"/Users/carvalho/tmp/bio/data_results",
			"numcores":			3
		}
