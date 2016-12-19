#!/usr/bin/env python
import os
import subprocess
import argparse
import sys

#use fancy argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('taskfile', type=argparse.FileType('r'))
#capture the rest of the arguments
parser.add_argument('rest', nargs=argparse.REMAINDER)
args = parser.parse_args()

num_tasks = sum(1 for line in args.taskfile)

script = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'dSQbatch.py')

cmd="sbatch --array=0-{} {} {} {}".format(num_tasks-1,
                                          " ".join(args.rest),
                                          script,
                                          args.taskfile.name)

ret=subprocess.call("foobar", shell=True)
sys.exit(ret)
