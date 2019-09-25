#!/usr/bin/env python
from __future__ import print_function
from subprocess import call, check_output
from datetime import datetime
from textwrap import fill 
from os import path
import os
import itertools
import argparse
import sys
import re

__version__ = 0.96

def safe_fill(text, wrap_width):
    if sys.__stdin__.isatty():
        return fill(text, wrap_width)
    else:
        return text

# Check if dSQ is being run interactively
if sys.__stdin__.isatty():
    # get terminal columns for wrapping
    term_rows, term_columns = [int(x) for x in check_output(["stty", "size"]).split()]
    if term_columns < 25:
        term_columns = 25
# non-interactive use will throw this error, but that is ok
else:
    term_columns = 25

#get slurm info
try:
    #get max configured array index
    slurm_conf = check_output(["scontrol", "show", "conf"], universal_newlines=True).split("\n")[:-1]
    max_array_size = [int(x.split("=")[1]) for x in slurm_conf if x.startswith("MaxArraySize")][0]
    
except FileNotFoundError as e:
    print("You don't appear to have slurm available. Exiting!")
    sys.exit(1)

desc = """Dead Simple Queue v{}
https://github.com/ycrc/dSQ
A simple utility for submitting a list of jobs as a job array using sbatch. The job file should specify one independent job you want to run per line. Empty lines or lines that begin with # will be ignored. Without specifying any additional sbatch arguments, some defaults will be set. Once the submission script is generated, you can run it as instructed.

dSQ will output a job_jobid_status.tsv file will contain the following tab-separated columns about your jobs:
Job_ID, Exit_Code, Hostname, Time_Started, Time_Ended, Time_Elapsed, Job

To generate a list of the jobs that didn't run or failed, use dSQAutopsy, or dsqa for short. 

Run sbatch --help or man sbatch for more slurm options. NOTE: The sbatch arguments you specify are for each individual job in your jobfile, NOT the entire job array.

Some useful sbatch arguments:
--mail-type=type           Email when job BEGIN, END, FAIL, REQUEUE, or ALL.
-p, --partition=partition  Partition requested.
-c, --cpus-per-task=ncpus  Number of cpu cores required per job.
--mem-per-cpu=MiB          Amount of memory per allocated cpu core per job.

""".format(__version__)
desc = "\n".join([safe_fill(x, term_columns-1) for x in str.splitlines(desc)])

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
    return ",".join(["{}-{}".format(x[0],x[1]) if x[0]!=x[1] else str(x[0]) for x in ranges]) 

# argument parsing
parser = argparse.ArgumentParser(description=desc,
                                 add_help=False, 
                                 usage="%(prog)s --job-file jobfile [dSQ args] [slurm args]", 
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 prog=sys.argv[0])

tmp = parser.add_argument_group("Required Arguments")
required_dsq = tmp.add_mutually_exclusive_group(required=True)
required_dsq.add_argument("--job-file",
                          metavar="jobs.txt",
                          nargs=1,
                          type=argparse.FileType("r"),
                          help="Job file, one self-contained job per line.")
# add old option names, but keep them hidden
required_dsq.add_argument("--taskfile",
                          nargs=1,
                          dest="job_file",
                          type=argparse.FileType("r"),
                          help=argparse.SUPPRESS)
required_dsq.add_argument("--jobfile",
                          nargs=1,
                          dest="job_file",
                          type=argparse.FileType("r"),
                          help=argparse.SUPPRESS)
# optional arguments
optional_dsq = parser.add_argument_group("Optional Arguments")
optional_dsq.add_argument("-h","--help",
                          action="help",
                          default=argparse.SUPPRESS,
                          help="Show this help message and exit.")
optional_dsq.add_argument("--version",
                          action="version",
                          version="%(prog)s {}".format(__version__))
optional_dsq.add_argument("--batch-file",
                          metavar="sub_script.sh",
                          nargs=1,
                          help=safe_fill("Name for batch script file. Defaults to dsq-jobfile-YYYY-MM-DD.sh", term_columns-24))
optional_dsq.add_argument("-J", "--job-name",
                          metavar="jobname",
                          nargs=1,
                          help="Name of your job array. Defaults to dsq-jobfile")
optional_dsq.add_argument("--max-jobs",
                          metavar="number",
                          nargs=1,
                          help="Maximum number of simultaneously running jobs from the job array.")
optional_dsq.add_argument("-o", "--output",
                          nargs=1,
                          metavar="fmt_string",
                          help=safe_fill("Slurm output file pattern. There will be one file per line in your job file. To suppress slurm out files, set this to /dev/null. Defaults to dsq-jobfile-%%A_%%a-%%N.out", term_columns-24))
optional_dsq.add_argument("--status-dir",
                          metavar="dir",
                          nargs=1,
                          help="Directory to save the job_jobid_status.tsv file to. Defaults to working directory.")
optional_dsq.add_argument("--stdout",
                          action="store_true",
                          help=argparse.SUPPRESS)
optional_dsq.add_argument("--submit",
                          action="store_true",
                          help="Submit the job array on the fly instead of creating a submission script.")
# silently allow overriding --array, otherwise we calculate that
optional_dsq.add_argument("-a", "--array",
                          nargs=1,
                          help=argparse.SUPPRESS)

args, user_slurm_args = parser.parse_known_args()

# organize job info into a dict
job_info = {}
job_info["max_array_size"] = max_array_size
job_info["max_jobs"] = args.max_jobs
job_info["num_jobs"] = 0
job_info["job_id_list"] = []
job_info["run_script"] = path.join(path.dirname(path.abspath(sys.argv[0])), "dSQBatch.py")
job_info["job_file_name"] = path.abspath(args.job_file[0].name)
job_info["slurm_args"] = {}
job_info["user_slurm_args"] = " ".join(user_slurm_args)
job_info["job_file_no_ext"] = path.splitext(path.basename(job_info["job_file_name"]))[0]
job_info["today"] = datetime.now().strftime("%Y-%m-%d")

# allow explicit setting of --array
if args.array is not None:
    job_info["array_range"] = args.array
    job_info["array_fmt_width"] = 2
    job_info["num_jobs"] = 1
else:
    # otherwise set it based on job file
    for i, line in enumerate(args.job_file[0]):
        if not (line.startswith("#") or line.rstrip() == ""):
            job_info["job_id_list"].append(i)
            job_info["num_jobs"]+=1
    job_info["max_array_idx"] = job_info["job_id_list"][-1]
    job_info["array_range"] = format_range(job_info["job_id_list"])

    # quit if we have too many array jobs
    if job_info["max_array_idx"] > job_info["max_array_size"]:
        print(safe_fill("Your job file would result in a job array with a maximum index of {max_array_idx}. This exceeds allowed array size of {max_array_size}. Split the jobs into chunks that are smaller than {max_array_size}, or do more per job.".format(**job_info), term_columns-1))
        sys.exit(1)
    job_info["array_fmt_width"] = len(str(job_info["max_array_idx"]))

# make sure there are jobs to submit
if job_info["num_jobs"] == 0:
    sys.stderr.write("No jobs found in {job_file_name}\n".format(**job_info))
    sys.exit(1)

# set output file format
if args.output is not None:
    job_info["slurm_args"]["--output"] = args.output[0]
else:
    job_info["slurm_args"]["--output"] = "dsq-{job_file_no_ext}-%A_%{array_fmt_width}a-%N.out".format(**job_info)

# set ouput directory
if args.status_dir is not None:
    job_info["status_dir"] = path.abspath(args.status_dir[0])
else:
    job_info["status_dir"] = path.abspath('./')
if not os.access(job_info["status_dir"], os.W_OK | os.X_OK):
    sys.stderr.write("{status_dir} doesn't appear to be a writeable directory.\n".format(**job_info))
    sys.exit(1)

# set array range string
if job_info["max_jobs"] == None:
    job_info["slurm_args"]["--array"] = job_info["array_range"]
else:
    job_info["max_jobs"] = args.max_jobs[0]
    job_info["slurm_args"]["--array"] = "{array_range}%{max_jobs}".format(**job_info)

# set default job name if not explicitly set
if args.job_name is not None:
    job_info["slurm_args"]["--job-name"] = args.job_name[0]
else:
    job_info["slurm_args"]["--job-name"] = "dsq-{job_file_no_ext}".format(**job_info)

# set batch script name
if args.stdout:
    job_info["batch_script_out"] = sys.stdout
else:
    try:
        if args.batch_file is not None:
            job_info["batch_script_out"] = open(args.batch_file[0], 'w')
        else:
            job_info["batch_script_out"] = open("dsq-{job_file_no_ext}-{today}.sh".format(**job_info), 'w')
    except Exception as e:
            print("Error: Couldn't open {batch_script_out} for writing. ".format(**job_info), e)

# submit or print the job script
if args.submit:

    job_info["cli_args"] = ""

    for option, value in job_info["slurm_args"].items():
        job_info["cli_args"] += " %s=%s" % (option, value)

    cmd = "sbatch {cli_args} {user_slurm_args} {run_script} {job_file_name} {status_dir}".format(**job_info)
    # print("submitting:\n {}".format(cmd))
    ret = call(cmd, shell=True)
    sys.exit(ret)

else:
    print("#!/bin/bash", file=job_info["batch_script_out"]) 
    for option, value in job_info["slurm_args"].items():
        print("#SBATCH {} {}".format(option, value), file=job_info["batch_script_out"])
    if len(job_info["user_slurm_args"]) > 0:
        print("#SBATCH {user_slurm_args}".format(**job_info), file=job_info["batch_script_out"])
    print("\n# DO NOT EDIT LINE BELOW".format(**job_info), file=job_info["batch_script_out"])
    print("{run_script} {job_file_name} {status_dir}\n".format(**job_info), file=job_info["batch_script_out"])
    if not args.stdout:
        print("Batch script generated. To submit your jobs, run:\n sbatch {}".format(job_info["batch_script_out"].name))

