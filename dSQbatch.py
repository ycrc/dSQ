#!/bin/env python

import subprocess, time, os, sys

## taskfile is a SQ style task file
taskfile=sys.argv[1]

jid=int(os.environ.get('SLURM_ARRAY_JOB_ID'))
tid=int(os.environ.get('SLURM_ARRAY_TASK_ID'))

## use task_id to get my task out of taskfile
cmds=open(taskfile).readlines()
mycmd=cmds[tid].strip()

# sanity check that size of array job matches taskfile.  
assert(len(cmds)==int(os.environ.get('SLURM_ARRAY_TASK_MAX'))+1)

st=time.time()
ret=subprocess.call(mycmd, shell=True)
et=time.time()

if ret!=0:
    open("job_%d.REMAINING" % jid, "a").write("%s\n" % (mycmd, ))

open("job_%d.STATUS" % jid, "a").write("%d\t%d\t%d\t%d\t%d\t\"%s\"\n" % (tid, ret, st, et, et-st, mycmd))

sys.exit(ret)
