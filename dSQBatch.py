#!/bin/env python
import os
import sys
import time
from datetime import datetime
import subprocess

## taskfile is a SQ style task file
taskfile = sys.argv[1]

jid = int(os.environ.get('SLURM_ARRAY_JOB_ID'))
tid = int(os.environ.get('SLURM_ARRAY_TASK_ID'))

# use task_id to get my task out of taskfile
with open(taskfile, 'r') as tf:
    for i, l in enumerate(tf):
        if i == tid:
            mycmd=l.strip()
            break

# run task and track its execution time
st = datetime.now()
ret = subprocess.call(mycmd, shell=True)
et = datetime.now()

# if it didn't finish successfully
if ret != 0:
    with open("job_{:d}.REMAINING".format(jid), "a") as out_remain:
        out_remain.write(mycmd+"\n")

out_cols = ["Task_ID", "Exit_Code", "T_Start", "T_End", "T_Elapsed", "Task"]
time_fmt = "%Y-%m-%d %H:%M:%S"
time_start = st.strftime(time_fmt)
time_end = et.strftime(time_fmt)
time_elapsed = (et-st).total_seconds()
out_dict = dict(zip(out_cols, 
                    [tid, ret, time_start, time_end, time_elapsed, mycmd]))

with open("job_{}.STATUS".format(jid, tid), "a") as out_status:
    out_status.write("{Task_ID}\t{Exit_Code}\t{T_Start}\t{T_End}\t{T_Elapsed:.02f}\t{Task}\n".format(**out_dict))

sys.exit(ret)
