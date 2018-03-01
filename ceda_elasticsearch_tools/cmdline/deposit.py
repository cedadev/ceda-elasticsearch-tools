"""
Script to be used as a cron job to keep elasticsearch up-to-date with arrivals.

Usage:
    deposit.py --help
    deposit.py --version
    deposit.py (-d DIR | --dir DIR )(-l LEVEL | --level LEVEL)(-i INDEX | --index INDEX)[--logdir LOGDIR]

Options:
    --help              Display help.
    --version           Show Version.
    -d --dir            Directory to put the lists created from deposit logs and to read from for the scanning.
    -l --level          Elasticsearch detail level
    -logdir             Logging directory
    -i --index          Index to modify





"""
from docopt import docopt

from ceda_elasticsearch_tools.core.log_reader import DepositLog
from ceda_elasticsearch_tools.cmdline import __version__
import os
import subprocess
import logging
import hashlib
import simplejson as json
from elasticsearch import Elasticsearch


def setup_logging(config):
    if not config['LOGDIR']:
        config['LOGDIR'] = 'deposit_cron_log'

    if not os.path.isdir(config["LOGDIR"]):
        os.makedirs(config["LOGDIR"])

    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(os.path.join(config['LOGDIR'], 'elasticsearch_deposit_cron.log'))
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


def delete_json(file_list):
    bulk_delete_json = ""

    for file in file_list:
        file_id = hashlib.sha1(file).hexdigest()
        bulk_delete_json += json.dumps({"delete": {"_index": "delete-test", "file": "file", "_id": file_id}}) + '\n'

    return bulk_delete_json



def main():
    config = docopt(__doc__, version=__version__)

    logger = setup_logging(config)

    # Check if output directory exists.
    if not os.path.isdir(config['DIR']):
        os.makedirs(config['DIR'])

    dl = DepositLog()

    logger.info("Scanning deposit log: {}".format(dl.filename))

    deposit_output = os.path.splitext(dl.filename)[0] + "_DEPOSIT.txt"
    delete_output = os.path.splitext(dl.filename)[0] + "_REMOVE.txt"

    # Check if this file has already been scanned and put into elasticsearch
    if deposit_output in os.listdir(config['DIR']):
        # File has been scanned, log messages and exit
        logger.info("Deposit log has already been scanned and put into elasticsearch")
        logger.info("Exiting script")

    else:
        # File has not yet been scanned. Proceed to push data to elasticsearch
        if dl.deposit_list:

            dl.write_filelist(os.path.join(config['DIR'], deposit_output))

            command = "/home/badc/software/fbs/venv-ceda-fbs/bin/python ceda-fbs/python/src/fbs/cmdline/scan_dataset.py -f {dataset} -n {num_files} -s 0 -l {level} i {index}".format(
                dataset=os.path.join("datasets", deposit_output),
                num_files=len(dl.deposit_list),
                level=config["LEVEL"],
                index=config["INDEX"]
            )

            logger.debug("Running command: {}".format(command))
            try:
                subprocess.call(command, shell=True)
            except Exception, e:
                logger.error("Elasticsearch update failed: " + str(e))

            logger.info(
                "Deposit log has been sent to elasticsearch. Total files in log: {}. Check fbs log for output".format(
                    len(dl.deposit_list)))
        else:
            logger.info("No files to add")

    if delete_output in os.listdir(config['DIR']):
        # File has been scanned, log messages and exit
        logger.info("Deposit log has already been scanned and relevant files deleted from elasticsearch")
        logger.info("Exiting script")

    else:
        # File has not yet been scanned. Proceed to remove relevant files from elasticsearch

        if dl.deletion_list:
            success = 0
            fail = 0

            # Create json to request deletion
            delete_request = delete_json(dl.deletion_list)
            es = Elasticsearch([{"host":"jasmin-es1.ceda.ac.uk","port":9200}])
            r = es.bulk(index=config["INDEX"], body=delete_request)

            # if there were no errors log and exit
            if r["errors"] == "false":
                logger.info("{} files successfully deleted from index: {}".format(len(dl.deletion_list),config["INDEX"]))

            # Log errors
            else:
                for item in r["items"]:
                    success += item["delete"]["_shards"]["successful"]
                    fail += item["delete"]["_shards"]["failed"]
                    logger.error("Deletion failed. Id: {}".format(item["delete"]["_id"]))

                logger.info("Successfully deleted: {} Deletion failed: {}".format(success,fail))

        # No files tagged for removal
        logger.info("There were no files marked for deletion in this round.")

