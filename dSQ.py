#!/usr/bin/env python
import os
import subprocess
import argparse
import sys

desc = """Dead Simple Queue
A simple utility for submitting a list of tasks as a job array, using sbatch. Tasks that return non-zero exit codes will be output to the job_<slurm job id>.REMAINING file. The job_<slurm job id>.STATUS file will contain info about the tasks run and contains the following tab-separated columns:
Task_ID, Exit_Code, Time_Started, Time_Ended, Time_Elapsed, Task

run sbatch --help for more info on sbatch options."""

#use fancy argument parsing
parser = argparse.ArgumentParser(description=desc, usage='%(prog)s taskfile [slurm args] ...', 
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('taskfile', type=argparse.FileType('r'), help="Task file, one task per line")
#capture the rest of the arguments
parser.add_argument('slurm_args', nargs=argparse.REMAINDER, help="flags and arguments to sbatch") 
args = parser.parse_args()

num_tasks = sum(1 for line in args.taskfile)

script = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'dSQbatch.py')

cmd="sbatch --array=0-{} {} {} {}".format(num_tasks-1,
                                          " ".join(args.slurm_args),
                                          script,
                                          args.taskfile.name)

ret=subprocess.call(cmd, shell=True)
sys.exit(ret)
