# biocomp - RAXML/ASTRAL/ETC Workflow

How to use:
* Create a SCRATCH directory.
* git clone git@github.com:diegomcarvalho/biocomp.git
* link the data directory under the biocomp directory. It should be a symbolic link to a scratch directory with all baselines.
* submit job.slrm

## The Pars scrip expect baselines files with the following directory structure
* baseline
	* astral
	* bucky-output
	* input
		* nexus
			* nexus files...
		* phylip
			* phylp files...
	* raxml
		* bootstrap


