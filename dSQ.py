#!/bin/env python

import sys, subprocess

taskfile=sys.argv[1]
slurmflags=sys.argv[2:]

tasks=len(open(taskfile).readlines())

cmd="sbatch --array=0-%d %s dSQbatch.py %s" % (tasks-1, " ".join(slurmflags), taskfile)

subprocess.call(cmd, shell=True)


