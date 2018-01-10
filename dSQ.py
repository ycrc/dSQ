#!/usr/bin/env python
from subprocess import call, check_output
from textwrap import wrap
from os import path
import itertools
import argparse
import sys
import re

__version__ = 0.9

#get slurm info
try:
    #get max configured array index
    slurm_conf = check_output(['scontrol', 'show', 'conf'], universal_newlines=True).split('\n')[:-1]
    MaxArraySize = [int(x.split('=')[1]) for x in slurm_conf if x.startswith('MaxArraySize')][0]
    
    #get unique list of partitions, maintaining order
    partitions_nu = [x.split()[0] for x in check_output(['sinfo', '-h'], universal_newlines=True).split('\n')[:-1]]
    parts = set()
    partitions = [x for x in partitions_nu if x not in parts and not parts.add(x)]
    
except FileNotFoundError as e:
    print("You don't appear to have slurm available. Exiting!")
    sys.exit(1)

desc = """Dead Simple Queue v{}
https://github.com/ycrc/dSQ
A simple utility for submitting a list of jobss as a job array using sbatch.
The job file should specify one independent job you want to run per line. 
Empty lines or lines that begin with # will be ignored. Without specifying
any additional sbatch arguments, some defaults will be set. To generate a 
list of the jobss that didn't run or failed, use dSQAutopsy

Output:
The job_<slurm job id>_status.tsv file will contain the following tab-separated
columns about your jobs:
Job_ID, Exit_Code, Time_Started, Time_Ended, Time_Elapsed, Job

There appear to be the following partitions on this cluster (* means default):
{}

Note: The sbatch arguments you specify are for each jobs in your jobfile, 
not the entire job array.

Some useful sbatch arguments:
--mail-type=type            notify on state change: BEGIN, END, FAIL or ALL
--mail-user=user            who to send email notification for job state
                            changes
-p, --partition=partition   partition requested
-c, --cpus-per-task=ncpus   number of cpu cores required 
--mem-per-cpu=MB            amount of memory per allocated cpu core
                              --mem >= --mem-per-cpu if --mem is specified.
""".format(__version__, '\n'.join(wrap(', '.join(partitions), 80)))

slurm_flag_dict = {'-J': '--job-name',
                   '-n': '--ntasks',
                   '-c': '--cpus-per-task'}


# helper functions for array range formatting
# collapse job numbers in job file to ranges
def _collapse_ranges(jobnums):
    # takes a list of numbers, returns tuples of numbers that specify representative ranges
    # inclusive
    for i, t in itertools.groupby(enumerate(jobnums), lambda tx: tx[1]-tx[0]):
        t = list(t)
        yield t[0][1], t[-1][1]


# format job ranges
def format_range(jobnums):
    ranges = list(_collapse_ranges(jobnums))
    return ','.join(['{}-{}'.format(x[0],x[1]) if x[0]!=x[1] else str(x[0]) for x in ranges]) 


# put back together slurm arguments
def parse_user_slurm_args(job_info, arg_list):
    
    extra_slurm_args = []

    i = 0
    while i < len(arg_list):
        arg = arg_list[i]

        if arg.startswith('--'):
            key, value = arg.split('=')

            if key in job_info['slurm_args'].keys():
                job_info['slurm_args'][key] = value
            else:
                extra_slurm_args.append(arg)

        elif arg.startswith('-') :
            i += 1
            value = arg_list[i]
            if arg in slurm_flag_dict.keys():
                job_info['slurm_args'][slurm_flag_dict[arg]] = value
            else:
                extra_slurm_args.append(arg+' '+value)
        
        else:
            sys.exit("Error parsing arguments")

        i += 1

    job_info['slurm_args']['extra'] = extra_slurm_args
    
# try getting user's email for job info forwarding
def get_user_email():
    email_match = None
    forward_file = path.join(path.expanduser('~'), '.forward')
    if path.isfile(forward_file):
        email = open(forward_file, 'r').readline().rstrip()
        emailre = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        email_match = re.match(emailre, email)
    if email_match is not None:
        return email_match.group(0)
    else:
        return None

# set defaults for the lazy
def set_defaults(job_info):
    job_info['slurm_args'] = {'--job-name' : job_info['jobfile_name'],
                             '--ntasks':'1',
                             '--cpus-per-task':'1',
                             }
                            
    uemail = get_user_email()
    if uemail is not None:
        job_info['email'] = uemail
        job_info['slurm_args']['--mail-type'] = 'ALL'
        job_info['slurm_args']['--mail-user'] = job_info['email']

# argument parsing
parser = argparse.ArgumentParser(description=desc,
                                 add_help=False, 
                                 usage='%(prog)s --jobfile jobfile [dSQ args] [slurm args]', 
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 prog='dSQ')
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
optional_dsq.add_argument('--max-jobs',
                          nargs=1,
                          help='Maximum number of simultaneously running jobs from the job array')
required_dsq.add_argument('--jobfile',
                          nargs=1,
                          required=True,
                          type=argparse.FileType('r'),
                          help='Job file, one job per line')

args, user_slurm_args = parser.parse_known_args()

#organize job info
job_info = {}
job_info['max_array_size'] = MaxArraySize
job_info['max_jobs'] = args.max_jobs
job_info['num_jobs'] = 0
job_info['job_id_list'] = []
job_info['script'] = path.join(path.dirname(path.abspath(sys.argv[0])), 'dSQBatch.py')
job_info['jobfile_name'] = args.jobfile[0].name

# set defaults
set_defaults(job_info)

# pull in custom user slurm args
parse_user_slurm_args(job_info, user_slurm_args)

#get job array IDs
for i, line in enumerate(args.jobfile[0]):
    if not (line.startswith('#') or line.rstrip() == ''):
        job_info['job_id_list'].append(i)
        job_info['num_jobs']+=1
job_info['max_array_idx'] = job_info['job_id_list'][-1]

#quit if we have too many array jobs
if job_info['max_array_idx'] > job_info['max_array_size']:
    print('Your job file would result in a job array with a maximum index of {max_array_idx}. This exceeds allowed array size of {max_array_size}. Please split the jobs into chunks that are smaller than {max_array_size}.'.format(**job_info))
    sys.exit(1)

#make sure there are jobs to submit
if job_info['num_jobs'] == 0:
    sys.stderr.write('No jobs found in {jobfile_name}\n'.format(**job_info))
    sys.exit(1)
job_info['array_range'] = format_range(job_info['job_id_list'])

#set array range string
if job_info['max_jobs'] == None:
    job_info['slurm_args']['--array'] = job_info['array_range']
else:
    job_info['max_jobs'] = args.max_jobs[0]
    job_info['slurm_args']['--array'] = '{array_range}%{max_jobs}'.format(**job_info)


#submit or print the job script
if args.submit:

    job_info['cli_args'] = ''

    for option, value in job_info['slurm_args'].items():
        if option == 'extra':
            job_info['cli_args'] += ' '+' '.join(value)
        else:
            job_info['cli_args'] += ' %s=%s' % (option, value)

    cmd = 'sbatch {cli_args} {script} {jobfile_name}'.format(**job_info)
    print('submitting:\n {}'.format(cmd))
    ret = call(cmd, shell=True)
    sys.exit(ret)

else:
    
    print('#!/bin/bash\n')
    for option, value in job_info['slurm_args'].items():
        if option == 'extra':
            for extra_option in value:
                print('#SBATCH %s' % extra_option)
        else:
            print('#SBATCH %s=%s' % (option, value))


    print('\n{script} {jobfile_name}'.format(**job_info))

