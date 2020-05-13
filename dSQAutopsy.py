#!/usr/bin/env python
from __future__ import print_function
from collections import defaultdict
from itertools import groupby
from os import path
from subprocess import call, check_output
from textwrap import fill
import argparse
import os
import sys

__version__ = 1.02
array_state_header = ["JobID", "State"]
sacct_cmd = ["sacct", 
             "-o" + ",".join(array_state_header),
             "-nXPj"]
possible_states = ["BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", 
                   "PENDING", "PREEMPTED", "RUNNING", "REQUEUED", "RESIZING", "REVOKED", "SUSPENDED", "TIMEOUT"]

def collapse_ranges(i):
    for a, b in groupby(enumerate(i), lambda pair: pair[1] - pair[0]):
        b = list(b)
        if b[0][1] == b[-1][1]:
            yield "{}".format(b[0][1])
        else:
            yield "{}-{}".format(b[0][1], b[-1][1])

def expand_ranges(idx_range):
    if "[" in idx_range:
        start = idx_range.find('[')+1
        end = idx_range.find(']') if idx_range.find('%') == -1 else idx_range.find('%')
        for sub_idx in idx_range[start:end].split(","):
            if "-" not in sub_idx:
                yield int(sub_idx)
            else:
                low, high = sub_idx.split("-", 1)
                for i in range(int(low), int(high) + 1):
                    yield int(i)
    else:
        yield int(idx_range)

def safe_fill(text, wrap_width):
    if sys.__stdin__.isatty():
        return fill(text, wrap_width)
    else:
        return text

def get_state_status(jid, rerun_states):
    state_summary = defaultdict(lambda: 0)
    array_states = defaultdict(lambda: [])
    state_summary_header = ["State", "Num_Jobs", "Indices"]
    reruns = []
    sacct_cmd.append(job_id)
    try:
        sacct_output = check_output(sacct_cmd).decode().split("\n")
    except Exception as e:
        # give up if we hit an error
        print("Error looking up job {}.".format(job_id), file=sys.stderr)
        sys.exit(1)
    column_lengths = dict(zip(state_summary_header, [len(x)+2 for x in state_summary_header]))
    # if there is job info
    if len(sacct_output)>=1:
        for l in sacct_output:
            split_line = l.split("|")
            if len(split_line) == len(array_state_header):
                line_dict = dict(zip(array_state_header,split_line))
                # track column widths for pretty printing
                if len(line_dict["State"])+2 > column_lengths["State"]:
                    column_lengths["State"]=len(line_dict["State"])+2
                if "_" in line_dict["JobID"]:
                    # track array idx
                    array_ids = list(expand_ranges(line_dict["JobID"].split("_")[1]))
                    array_states[line_dict["State"]] = array_states[line_dict["State"]]+ array_ids
                    state_summary[line_dict["State"]]+=len(array_ids)
                    if any([line_dict["State"].startswith(x) for x in rerun_states]):
                        # add them to the reruns list if desired
                        # some states can have info appended, e.g.
                        # "CANCELLED by 124412", but want to treat it as CANCELLED
                        reruns = reruns + array_ids
                else:
                    print("{} does not look like a job array.".format(jid), file=sys.stderr)
                    sys.exit(1)

    for state in array_states:
        array_states[state] = ",".join(collapse_ranges(sorted(array_states[state])))
        if len(array_states[state])+2 > column_lengths["Indices"]:
            # track column widths for pretty printing
            column_lengths["Indices"] = len(array_states[state])+2

    print("State Summary for Array {}".format(job_id), file=sys.stderr)
    summary_template = "{{:<{}}}{{:^{}}}{{:<{}}}".format(*[column_lengths[x] for x in state_summary_header])
    print(summary_template.format(*state_summary_header), file=sys.stderr)
    print(summary_template.format(*["-" * len(x) for x in state_summary_header]), file=sys.stderr)
    for state in sorted(state_summary, key=state_summary.get, reverse=True):
        print(summary_template.format(state, state_summary[state], array_states[state]), file=sys.stderr)
    return reruns

# get terminal columns for wrapping
# Check if dSQ is being run interactively
if sys.__stdin__.isatty():
    # get terminal columns for wrapping
    term_rows, term_columns = [int(x) for x in check_output(["stty", "size"]).split()]
    if term_columns < 25:
        term_columns = 25
else:
    term_columns = 25

desc = """Dead Simple Queue Autopsy v{}
https://github.com/ycrc/dSQ
A helper script for analyzing the state of your dSQ jobs and identifying which you want to re-run. Specify the Slurm Job ID to see the state status. If you would like to generate a new job file with the jobs that need re-running, also specify your original job file and optionally the statuses you want to re-run. You can then redirect the output to a new file.

For more in-depth job stats use sacct, seff, or seff-array.

Example usage:

dsqa -j 1111 
dsqa -j 1243 -f jobs.txt -s NODE_FAIL,PREEMPTED > rerun_jobs.txt

""".format(__version__)
desc = "\n".join([safe_fill(x, term_columns-1) for x in str.splitlines(desc)])

# argument parsing
parser = argparse.ArgumentParser(description=desc,
                                 usage="%(prog)s --job-id jobid [--job-file jobfile.txt [--states STATES] > new_jobs.txt]", 
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 prog=path.basename(sys.argv[0]))
parser.add_argument("-v","--version",
                    action="version",
                    version="%(prog)s {}".format(__version__))
parser.add_argument("-j", "--job-id",
                    nargs=1,
                    required=True,
                    help="The Job ID of a running or completed dSQ Array")
parser.add_argument("-f", "--job-file",
                    nargs=1,
                    help="Job file, one job per line (not your job submission script).")
parser.add_argument("-s", "--states",
                    nargs=1,
                    default="CANCELLED,NODE_FAIL,PREEMPTED",
                    help="Comma separated list of states to use for re-writing job file. Default: CANCELLED,NODE_FAIL,PREEMPTED")

args = parser.parse_args()
job_id = args.job_id[0]
rerun_states = []
for state in args.states.split(","):
    if state in possible_states:
        rerun_states.append(state)
    else:
        print("Unknown state: {}.".format(state), file=sys.stderr)
        print("Choose from {}.".format(",".join(possible_states)), file=sys.stderr)
        sys.exit(1)

print_reruns = False
if (args.job_file):
    try:
        job_file = open(args.job_file[0], "r")
        print_reruns = True
    except Exception as e:
        print("Could not open {}.".format(args.job_file[0]), file=sys.stderr)
        sys.exit(1)

reruns = get_state_status(job_id, rerun_states)
if print_reruns:
    for i, line in enumerate(job_file):
        if i in reruns:
            print(line.rstrip())
