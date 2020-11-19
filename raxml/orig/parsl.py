#!/usr/bin/env python
import sys
import os
from parsl import load, python_app, bash_app
from configs.config import config
from pathlib import Path

load(config)

script_dir = sys.arg[1]
sec_dir = sys.arg[2]
astral_dir = sys.arg[3]
@bash_app
def run_perl(script_dir, seq_dir, astral_dir, stdout=None):
    return 'perl {} --seqdir={} --astraldir={} > mylog 2>&1'.format(script_dir, seq_dir, astral_dir)

run_ = run_perl(script_dir, seq_dir, astral_dir)
run_.result()