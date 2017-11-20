"""
Submits jobs to run on lotus, monitors progress and prints a summary table at the end.

Usage:
    fbs_missing_files.py --help
    fbs_missing_files.py --version
    fbs_missing_files.py
                   (-d DIR         | --directory DIR     )
                   (-o OUTPUT       | --output OUTPUT )
                   (-i INDEX        | --index   INDEX )
                   [-h HOSTNAME     | --hostname HOSTNAME ]
                   [-b BLOCKSIZE    | --blocksize BLOCKSIZE ]
                   [--nolotus                               ]


Options:
    --help              Display help.
    --version           Show Version.
    -d  --directory     Directory to scan.
    -o  --output        Script output directory.
    -i  --index         Elasticsearch index to test.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -b  --blocksize     Number of files to chunk into bulk query [default: 800]
"""
from docopt import docopt

from tabulate import tabulate
import re
import os
from ceda_elasticsearch_tools.core import util
import subprocess
from cmdline import __version__
from time import sleep
from tqdm import tqdm
from datetime import datetime



SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))



def submit_jobs_to_lotus(filelist, config):

    for file in tqdm(filelist, desc="Submitting to Lotus" ):
        filepath = os.path.join(config["DIR"],file)

        task = "python {}/spot_checker.py -f {} -o  {} -i {}".format(
            SCRIPT_DIR, filepath, config["OUTPUT"], config["INDEX"])

        if config["HOSTNAME"]:
            task += " -h {}".format(config["HOSTNAME"])

        if config["BLOCKSIZE"]:
            task += " -b {}".format(config["BLOCKSIZE"])

        command = util._make_bsub_command(task)

        subprocess.call(command, shell=True)


def generate_summary(config):

    # Generate Summary Table
    summary = {}
    for file in os.listdir(config["OUTPUT"]):
            spot_name = file.split('_log.txt')[0]

            with open(os.path.join(config["OUTPUT"],file)) as input:
                    first_line = input.readline()
                    line = re.sub(r"[a-zA-z%]","",first_line).split(':')
                    parsed_values = [int(float(x.strip())) for x in line if x.strip()]

                    summary[spot_name] = parsed_values

    data = [[k]+i for k,i in summary.iteritems()]

    # Display table and print totals
    print tabulate(data, headers=["Spot","Files in Spot","Indexed","Missing","Percent Missing"])
    print "Total Files: {} Total Indexed: {} Total Missing: {}".format(sum([x[1] for x in data]),sum([x[2] for x in data]),sum([x[3] for x in data]))


def main():
    # Parse the command line arguments
    config = docopt(__doc__, version=__version__)

    if not config["--nolotus"]:
        # If --nolotus flag provided, skip the processing phase and generate the summary table.

        files = os.listdir(config["DIR"])
        total_files = len(files)
        pb = util.ProgressBar(total_files, label="Running jobs")

        submit_jobs_to_lotus(files,config)
        remaining_jobs = util.get_number_of_submitted_lotus_tasks()

        while remaining_jobs > 0:
            pb.running(total_files-remaining_jobs)
            sleep(5)
            remaining_jobs = util.get_number_of_submitted_lotus_tasks()

        pb.complete()

    generate_summary(config)