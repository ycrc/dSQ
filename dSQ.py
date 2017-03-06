#!/usr/bin/env python
from os import path
import subprocess
import itertools
import argparse
import sys
import re

__version__ = '0.3'
desc = """Dead Simple Queue v{}
https://github.com/ycrc/dSQ
A simple utility for submitting a list of tasks as a job array using sbatch.
Specify a task file and any sbatch parameters and dSQ will construct a job 
array submission for you. The task file should specify one independent, 
parallel task you want to run per line. Empty lines or lines that begin 
with # will be ignored. Without specifying any additional sbatch arguments, 
some defaults will be set.

Output:
Tasks that return non-zero exit codes will be output to the 
job_<slurm job id>.REMAINING file. The job_<slurm job id>.STATUS file 
will contain info about the tasks run and contains the following 
tab-separated columns:
Task_ID, Exit_Code, Time_Started, Time_Ended, Time_Elapsed, Task

run sbatch --help or see https://slurm.schedmd.com/sbatch.html
for a complete list of sbatch arguments.

Some useful sbatch aruments:
--mail-type=type            notify on state change: BEGIN, END, FAIL or ALL
--mail-user=user            who to send email notification for job state
                            changes
-p, --partition=partition   partition requested
-N, --nodes=N               number of nodes on which to run each task
--ntasks-per-node=n         number of tasks to invoke on each node
--ntasks-per-core=n         number of tasks to invoke on each core
-c, --cpus-per-task=ncpus   number of cores required per task
--mincpus=n                 minimum number of cores per node
--mem=MB                    amount of memory to request per node
--mem-per-cpu=MB            amount of memory per allocated cpu
                              --mem >= --mem-per-cpu if --mem is specified.
""".format(__version__)

#helper functions for array range formatting
#collapse task numbers in job file to ranges
def _collapse_ranges(tasknums):
    #takes a list of numbers, returns tuples of numbers that specify representative ranges
    #inclusive
    for i, t in itertools.groupby(enumerate(tasknums), lambda tx: tx[1]-tx[0]):
        t = list(t)
        yield t[0][1], t[-1][1]

#format job ranges
def format_range(tasknums):
    ranges = list(_collapse_ranges(tasknums))
    return ','.join(['{}-{}'.format(x[0],x[1]) if x[0]!=x[1] else str(x[0]) for x in ranges]) 

#put back together slurm arguments
def parse_extras(arg_list):
    better_list = []
    for arg in arg_list:
        if arg.startswith('-'):
            better_list.append(arg)
        else:
            better_list[-1] += ' ' + arg
    return better_list

#try getting user's email for job info forwarding
def get_user_email():
    forward_file = path.join(path.expanduser('~'), '.forward')
    if path.isfile(forward_file):
        email = open(forward_file, 'r').readline().rstrip()
        emailre = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        email_match = re.match(emailre, email)
    if email_match is not None:
        return email_match.group(0)
    else:
        return None

#argument parsing
parser = argparse.ArgumentParser(description=desc,
                                 add_help=False, 
                                 usage='%(prog)s --taskfile taskfile [dSQ args] [slurm args]', 
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 prog='dSQ.py')
required_dsq = parser.add_argument_group('Required dSQ arguments')
optional_dsq = parser.add_argument_group('Optional dSQ arguments')
optional_dsq.add_argument('-h','--help', 
                          action='help',
                          default=argparse.SUPPRESS,
                          help='show this help message and exit')
optional_dsq.add_argument('--version',
                          action='version',
                          version='%(prog)s {}'.format(__version__))
optional_dsq.add_argument('--submit',
                          action='store_true',
                          help='Submit the job array on the fly instead of printing to stdout.')
optional_dsq.add_argument('--max-tasks',
                          nargs=1,
                          help='Maximum number of simultaneously running tasks from the job array')
required_dsq.add_argument('--taskfile',
                          nargs=1,
                          required=True,
                          type=argparse.FileType('r'),
                          help='Task file, one task per line')
args, extra_args = parser.parse_known_args()

#organize job info
jobinfo = {}
jobinfo['max_tasks'] = args.max_tasks
jobinfo['num_tasks'] = 0
jobinfo['task_id_list'] = []
jobinfo['script'] = path.join(path.dirname(path.abspath(sys.argv[0])), 'dSQbatch.py')
jobinfo['taskfile_name'] = args.taskfile[0].name
jobinfo['slurm_args'] = parse_extras(extra_args)

#get job array IDs
for i, line in enumerate(args.taskfile[0]):
    if not (line.startswith('#') or line.rstrip() == ''):
        jobinfo['task_id_list'].append(i)
        jobinfo['num_tasks']+=1

#make sure there are tasks to submit
if jobinfo['num_tasks'] == 0:
    sys.stderr.write('No tasks found in {taskfile_name}\n'.format(**jobinfo))
    sys.exit(1)
jobinfo['array_range'] = format_range(jobinfo['task_id_list'])

#set some defaults for the lazy if they didnt specify any sbatch args
if len(jobinfo['slurm_args']) == 0:
    jobinfo['slurm_args'] = ['--partition=general',
                             '--job-name={taskfile_name}'.format(**jobinfo),
                             '--ntasks={num_tasks}'.format(**jobinfo),
                             '--cpus-per-task=1',
                             '--mem-per-cpu=1024'
                             ]
    #try to get user email
    uemail = get_user_email()
    if uemail is not None:
        jobinfo['email'] = uemail
        jobinfo['slurm_args'].append('--mail-type=ALL')
        jobinfo['slurm_args'].append('--mail-user={email}'.format(**jobinfo))


#set array range string
if jobinfo['max_tasks'] == None:
    jobinfo['slurm_args'] += [ '--array={array_range}'.format(**jobinfo) ]
else:
    jobinfo['max_tasks'] = args.max_tasks[0]
    jobinfo['slurm_args'] += [ '--array={array_range}%{max_tasks}'.format(**jobinfo) ]

#submit or print the job script
if args.submit:
    jobinfo['cli_args'] = ' '.join(jobinfo['slurm_args'])
    cmd = 'sbatch {cli_args} {script} {taskfile_name}'.format(**jobinfo)
    print('submitting:\n {}'.format(cmd))
    ret=subprocess.call(cmd, shell=True)
    sys.exit(ret)
else:
    print('#!/bin/bash\n')
    for option in jobinfo['slurm_args']:
        print('#SBATCH {}'.format(option))
    print('\n{script} {taskfile_name}'.format(**jobinfo))

