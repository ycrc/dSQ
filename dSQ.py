#!/bin/env python

import sys, subprocess, os

taskfile=sys.argv[1]
slurmflags=sys.argv[2:]

script=os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'dSQbatch.py')
tasks=len(open(taskfile).readlines())

cmd="sbatch --array=0-%d %s %s %s" % (tasks-1, " ".join(slurmflags), script, taskfile)

subprocess.call(cmd, shell=True)


