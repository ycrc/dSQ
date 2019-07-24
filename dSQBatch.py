#!/bin/env python
import os
import sys
import time
import signal
import platform
from functools import partial
from subprocess import Popen
from datetime import datetime

"""
This script accepts the name of a job file and a directory to save status info to. Meant to be called with submission scripts generated with dSQ.py.
"""


def forward_signal_to_child(pid, signum, frame):
    print("[dSQ]: ", pid, signum, frame)
    os.kill(pid, signum)

def exec_job(job_str):
    process = Popen(job_str, shell=True)
    signal.signal(signal.SIGCONT, partial(forward_signal_to_child, process.pid))
    signal.signal(signal.SIGTERM, partial(forward_signal_to_child, process.pid))
    return_code = process.wait()
    return(return_code)

# jobfile is a path to text file with jobs, 1 per line
jobfile = sys.argv[1]
# statusfile output directory
status_outdir = sys.argv[2]


jid = int(os.environ.get("SLURM_ARRAY_JOB_ID"))
tid = int(os.environ.get("SLURM_ARRAY_TASK_ID"))
# slurm calls individual job array indices "tasks"

hostname = platform.node()

# use task_id to get my job out of jobfile
with open(jobfile, 'r') as tf:
    for i, l in enumerate(tf):
        if i == tid:
            mycmd=l.strip()
            break

# run job and track its execution time
st = datetime.now()
ret = exec_job(mycmd)
et = datetime.now()

# set up job stats
out_cols = ["Array_Task_ID", "Exit_Code", "Hostname", "T_Start", "T_End", "T_Elapsed", "Task"]
time_fmt = "%Y-%m-%d %H:%M:%S"
time_start = st.strftime(time_fmt)
time_end = et.strftime(time_fmt)
time_elapsed = (et-st).total_seconds()
out_dict = dict(zip(out_cols, 
                    [tid, ret, hostname, time_start, time_end, time_elapsed, mycmd]))

# append status file with job stats
with open(os.path.join(status_outdir, "job_{}_status.tsv".format(jid)), "a") as out_status:
    out_status.write("{Array_Task_ID}\t{Exit_Code}\t{Hostname}\t{T_Start}\t{T_End}\t{T_Elapsed:.02f}\t{Task}\n".format(**out_dict))

sys.exit(ret)
