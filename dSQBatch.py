#!/bin/env python
import os
import sys
import time
import signal
from functools import partial
from subprocess import Popen
from datetime import datetime

def forward_signal_to_child(pid, signum, frame):
    print("dSQ: ", pid, signum, frame)
    os.kill(pid, signum)

def exec_task (task_str):
    process = Popen(task_str, shell=True)
    signal.signal(signal.SIGINT, partial(forward_signal_to_child, process.pid))
    signal.signal(signal.SIGTERM, partial(forward_signal_to_child, process.pid))
    signal.signal(signal.SIGHUP, partial(forward_signal_to_child, process.pid))
    signal.signal(signal.SIGQUIT, partial(forward_signal_to_child, process.pid))
    return_code = process.wait()
    return(return_code)

# taskfile is a SQ style task file
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
ret = exec_task(mycmd)
et = datetime.now()

# set up job stats
out_cols = ["Task_ID", "Exit_Code", "T_Start", "T_End", "T_Elapsed", "Task"]
time_fmt = "%Y-%m-%d %H:%M:%S"
time_start = st.strftime(time_fmt)
time_end = et.strftime(time_fmt)
time_elapsed = (et-st).total_seconds()
out_dict = dict(zip(out_cols, 
                    [tid, ret, time_start, time_end, time_elapsed, mycmd]))

# append status file with task stats
with open("job_{}_status.tsv".format(jid, tid), "a") as out_status:
    out_status.write("{Task_ID}\t{Exit_Code}\t{T_Start}\t{T_End}\t{T_Elapsed:.02f}\t{Task}\n".format(**out_dict))

sys.exit(ret)
