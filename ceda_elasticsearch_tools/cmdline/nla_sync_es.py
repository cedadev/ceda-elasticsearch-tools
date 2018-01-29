'''
file_on_tape.py

Updates the file location on the target elasticsearch index using the NLA as a baseline.
Files which are not visible on disk at the time of scanning will not be in ES. Use the
--index-doc flag to put these missing documents in elasticsearch.
If NLA status ONDISK or RESTORED and scan level is set > 1, create jobs for lotus to add
these files at the desired detail.

To be used as a command line tool.

Usage:
    file_on_tape.py INDEX
    file_on_tape.py INDEX [--port PORT]
    file_on_tape.py INDEX [--host HOST] [--port PORT]
    file_on_tape.py INDEX [--index-docs]
    file_on_tape.py INDEX [--level LEVEL ] [--index-docs]
    file_on_tape.py INDEX [--host HOST] [--port PORT] [--level LEVEL ] [--index-docs]
    file_on_tape.py --version

Options:
    -h --help       Show this screen.
    --version       Show version.
    --host          Elasticsearch host to target.
    --port          Elasticsearch port to target.
    --level         ceda-fbs scanning level.
    --index-docs    Index files not found at level 1 detail or if RESTORED create job for lotus.
'''
from docopt import docopt

from ceda_elasticsearch_tools.core.updater import ElasticsearchUpdater, ElasticsearchQuery
import requests
import pkg_resources
from time import sleep
import itertools, sys
from multiprocessing import Process
import os
import shutil

def loading(message):
    """
    Creates spinning loading bar
    :param message: Message to display followed by spinning bar
    """
    for i in itertools.cycle(['|', '/', '-', '\\', '|', '/', '-', '\\']):
        text = message + ": {}".format(i)
        sys.stdout.write(text)
        sys.stdout.flush()
        sys.stdout.write('\r')
        sleep(0.1)

def create_output_dir(output_dir, batch_dir):
    """
    Makes sure that the output directories are in place and clean, ready for the sync
    """
    print("Creating output dir")



    if os.path.isdir(OUTPUT_DIR):
        # Make sure to always start with fresh batch directory
        shutil.rmtree(BATCH_DIR)
        os.mkdir(BATCH_DIR)

    else:
        # Create the needed dirs
        os.mkdir(OUTPUT_DIR)
        os.mkdir(BATCH_DIR)
        os.mkdir(os.path.join(BATCH_DIR,'on_tape'))
        os.mkdir(os.path.join(BATCH_DIR,'on_disk'))


def download_data_from_nla(url):
    """
    Download the data lists from the NLA
    :param url: The url to get data from.
    :return: Two lists: files_t = Files on tape, files_d = Files on disk
    """
    p = Process(target=loading,args=("Retrieving list of files on tape from nla.ceda.ac.uk",))
    p.start()

    r = requests.get(url).json()

    files_t = [x["path"] for x in r["files"] if x["stage"] == 'T']
    files_d = [x["path"] for x in r["files"] if x["stage"] == 'D' or x["stage"] == 'R']

    p.terminate()
    print("")


    return files_t, files_d

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def main():
    '''
    Steps to achieve the desired result:
    1. Retrieve list of files on tape from NLA
    2. Search target index, retrieving IDs for files in the index and holding information about files not in the index.
    3. Use list of files which are in the index to update the location to on tape
    4. Dump list of files not contained in the index for further analysis.
    '''

    # Get command line arguments
    args = docopt(__doc__, version=pkg_resources.require("ceda_elasticsearch_tools")[0].version)

    if not args["--host"]:
        host = "jasmin-es1.ceda.ac.uk"
    else:
        host = args["HOST"]

    if not args["--port"]:
        port = 9200
    else:
        port = args["PORT"]

    index = args["INDEX"]

    OUTPUT_DIR = "nla_sync_files"
    BATCH_DIR = os.path.join(OUTPUT_DIR, 'batch_files')

    print("Script settings. ElasticSearch index: {} host: {} port: {}".format(index, host, port))

    # Get list of files on tape and disk
    url = "http://nla.ceda.ac.uk/nla_control/api/v1/files?stages=TDR"
    files_on_tape, files_on_disk = download_data_from_nla(url)

    # Create output directory
    create_output_dir(OUTPUT_DIR, BATCH_DIR)

    # Split files_on_tape into chunks of 10k and write to disk
    for i,files in enumerate(chunks(files_on_tape,10000)):
        with open(os.path.join(BATCH_DIR,"on_tape","on_tape_batch_{}.txt".format(i)),'w') as writer:
            writer.writelines([x+"\n" for x in files])

    for i,files in enumerate(chunks(files_on_disk,10000)):
        with open(os.path.join(BATCH_DIR,"on_disk","on_disk_batch_{}.txt".format(i)),'w') as writer:
            writer.writelines([x+"\n" for x in files])

    # # Update documents
    # print("Updating ElasticSearch")
    #
    # # Get file query for ceda_fbs
    # params, query = ElasticsearchQuery.ceda_fbs()
    #
    # _,_,missing_files_list = ElasticsearchUpdater(index=index,
    #                      host=host,
    #                      port=port
    #                      ).update_location(file_list=files, params=params, search_query=query, on_disk=False)

if __name__ == "__main__":

    main()
