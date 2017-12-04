"""
Script to be used as a cron job to keep elasticsearch up-to-date with arrivals.

Usage:
    deposit.py --help
    deposit.py --version
    deposit.py (-d DIR | --dir DIR )(-l LEVEL | --level LEVEL)[--logdir LOGDIR]

Options:
    --help              Display help.
    --version           Show Version.
    -d --dir            Directory to put the lists created from deposit logs and to read from for the scanning.
    -l --level          Elasticsearch detail level
    -logdir             Logging directory





"""
from docopt import docopt


from ceda_elasticsearch_tools.core.log_reader import DepositLog
from ceda_elasticsearch_tools.cmdline import __version__
import os
import subprocess
import logging

def setup_logging(config):

    if not config['LOGDIR']:
        config['LOGDIR'] = 'deposit_cron_log'

    if not os.path.isdir(config["LOGDIR"]):
        os.makedirs(config["LOGDIR"])

    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(os.path.join(config['LOGDIR'], 'elasticsearch_deposit_cron.log' ))
    formatter = logging.Formatter('%(levelname)s %(asctime)s $(name)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


def main():
    config = docopt(__doc__, version=__version__)

    logger = setup_logging(config)

    # Check if output directory exists.
    if not os.path.isdir(config['DIR']):
        os.makedirs(config['DIR'])

    dl = DepositLog()

    logger.info("Scanning deposit log: {}".format(dl.filename))

    output_name = os.path.splitext(dl.filename)[0] + ".txt"

    # Check if this file has already been scanned and put into elasticsearch
    if output_name in os.listdir(config['DIR']):
        # File has been scanned, log messages and exit
        logger.info("Deposit log has already been scanned and put into elasticsearch")
        logger.info("Exiting script")

    else:
        # File has not yet been scanned. Proceed to push data to elasticsearch
        dl.write_filelist(os.path.join(config['DIR'],output_name))

        command = "/home/badc/software/fbs/venv-ceda-fbs/bin/python ceda-fbs/python/src/fbs/cmdline/scan_dataset.py -f {dataset} -n {num_files} -s 0 -l {level}".format(
            dataset=os.path.join("datasets",output_name),
            num_files = len(dl.file_list),
            level = config["LEVEL"]
        )

        logger.debug("Running command: {}".format(command))
        try:
            subprocess.call(command, shell=True)
        except Exception, e:
            logger.error("Elasticsearch update failed: " + str(e))

        logger.info("Deposit log has been sent to elasticsearch. Total files in log: {}. Check fbs log for output".format(len(dl)))

