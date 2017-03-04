# Dead Simple Queue (dSQ)
Dead simple queue is a [slurm](https://slurm.schedmd.com/)-only successor to SimpleQueue. It wraps around slurm's [`sbatch`](https://slurm.schedmd.com/sbatch.html) to help you submit independent tasks as job arrays. It's primary advantage over SimpleQueue is that your job allocation will only ever use the resources needed to complete the remaining tasks.

## Task File:
First, you'll need to generate a task file. Each line of this task file needs to specify exactly what you want run for each task, including any modules that need to be loaded or modifications to your environment variables. Empty lines or lines that begin with `#` will be ignored when submitting your job array. **Note:** slurm jobs begin in the directory from which your job was submitted, so be wary of relative paths. This also means that you don't need to `cd` to the working directory if you submit your job there.

## Usage:
`dSQ.py` takes a few arguments, then passes the rest directly to sbatch, either by writing a script to stdout or by directly submitting the job for you. Without specifying any additional sbatch arguments, some defaults will be set. run `sbatch --help` or see https://slurm.schedmd.com/sbatch.html for more info on sbatch options.


```
dSQ.py --taskfile taskfile [dSQ args] [slurm args]

Required dSQ arguments:
  --taskfile TASKFILE   Task file, one task per line

Optional dSQ arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --submit              Submit the job array on the fly instead of printing to stdout.
  --max-tasks MAX_TASKS
                        Maximum number of simultaneously running tasks from the job array
```

## Output
Tasks that return non-zero exit codes will be output to the `job_<slurm job id>.REMAINING` file. The `job_<slurm job id>.STATUS` file will contain info about the tasks run and contains the following tab-separated columns:

| Task_ID | Exit_Code | Time_Started | Time_Ended | Time_Elapsed | Task |
| :------ | :-------- | :----------- | :--------- | :----------- | :--- |
