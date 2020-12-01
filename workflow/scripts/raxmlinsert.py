# python3

import os
import sys

with open("raxml.cmd","w") as f:
	f.write(str(os.getcwd()))
	f.write("\n")
	f.write(str(sys.argv))
	f.write("\n")
