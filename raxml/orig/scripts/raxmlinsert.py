# python3

import os
import sys

def createfile(varargs):
	with open("raxml.cmd","w") as f:
		f.write(str(os.getcwd()))
		f.write("\n")
	return

def raxmlinsert(varargs):
	with open("raxml.cmd","a") as f:
		for i in varargs:
			f.write(f"{i} ")
		f.write("\n")
	return

def notfound(varargs):
	print(f"{sys.argv[0]}: Command not found.")
	return

def help():
	print(f"{sys.argv[0]}: command.")

cmdtable ={
	"start": createfile,
	"raxml": raxmlinsert
}

def main():
	if len(sys.argv) == 1:
		help()
		return 0

	cmd = sys.argv[1]

	varargs = sys.argv[2:]
	
	f = cmdtable.get(cmd,notfound)

	f(varargs)

	return 0

if __name__ == "__main__":
	main()

