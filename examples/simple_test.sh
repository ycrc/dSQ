#!/bin/bash

#SBATCH --partition=admintest
#SBATCH --job-name=test_dsq
#SBATCH --ntasks=1 --nodes=1
#SBATCH -t 05:00

python2 ./dSQ.py
python2 ./dSQ.py --job-file sleepyjobs.txt
python2 ./dSQ.py --stdout --job-file sleepyjobs.txt
python2 ./dSQ.py --help

module load miniconda
source activate py3
python3 ./dSQ.py
python3 ./dSQ.py --job-file sleepyjobs.txt
python3 ./dSQ.py --stdout --job-file sleepyjobs.txt
python3 ./dSQ.py --help
