'''
nla_sync_fbs.py

Updates the file location on the target elasticsearch index using the NLA as a baseline.
Files which are not visible on disk at the time of scanning will not be in ES. Use the
--index-doc flag to put these missing documents in elasticsearch.
If NLA status ONDISK or RESTORED and scan level is set > 1, create jobs for lotus to add
these files at the desired detail.

To be used as a command line tool.

Usage:
    nla_sync_es.py INDEX
    nla_sync_es.py INDEX [--host HOST]
    nla_sync_es.py INDEX [--host HOST] [--port PORT]
    nla_sync_es.py INDEX [--host HOST] [--port PORT] [--index-docs] [--no-scan]
    nla_sync_es.py --version

Options:
    -h --help       Show this screen.
    --version       Show version.
    --host          Elasticsearch host to target.
    --port          Elasticsearch port to target.
    --index-docs    Index files not found at level 1 detail or if available on disk, write to dir ready for FBS code.
    --no-scan       Don't download new list. Used to capture failed jobs.
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
from itertools import islice
import simplejson as json
import subprocess

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
    print("Creating output dir: {}".format(output_dir))
    if os.path.isdir(output_dir):
        # Make sure to always start with fresh batch and missing files directory
        print("Deleting existing batch dir and missing files dir")
        shutil.rmtree(batch_dir)
        shutil.rmtree(os.path.join(output_dir,'missing_files_on_disk'))

        print("Creating new batch dir and missing files dir")
        os.mkdir(os.path.join(output_dir,'missing_files_on_disk'))
        os.mkdir(batch_dir)
        os.mkdir(os.path.join(batch_dir, 'on_tape'))
        os.mkdir(os.path.join(batch_dir, 'on_disk'))

    else:
        # Create the needed dirs
        os.mkdir(output_dir)
        os.mkdir(os.path.join(output_dir,'missing_files_on_disk'))
        os.mkdir(batch_dir)
        os.mkdir(os.path.join(batch_dir,'on_tape'))
        os.mkdir(os.path.join(batch_dir,'on_disk'))


def download_data_from_nla(url):
    """
    Download the data lists from the NLA
    :param url: The url to get data from.
    :return: Two dicts: files_t = Files on tape, files_d = Files on disk
    """
    p = Process(target=loading,args=("Retrieving list of files on tape from nla.ceda.ac.uk",))
    p.start()

    r = requests.get(url).json()

    files_t = {file['path']:file['size'] for file in r['files'] if file['stage']=='T'}
    files_d = {file['path']:file['size'] for file in r['files'] if file['stage']=='D' or file['stage']=='R'}

    # for file in r['files']
    #
    # files_t = [x["path"] for x in r["files"] if x["stage"] == 'T']
    # files_d = [x["path"] for x in r["files"] if x["stage"] == 'D' or x["stage"] == 'R']

    p.terminate()
    print("")


    return files_t, files_d

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunk_dict(d, n):
    '''Yield n-sized chuncks from d'''
    it = iter(d)
    for i in xrange(0, len(d), n):
        yield {k:d[k] for k in islice(it, n)}


def main():
    """

    """

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


    if not args['--no-scan']:
        # Get list of files on tape and disk
        url = "http://nla.ceda.ac.uk/nla_control/api/v1/files?stages=TDR"
        files_on_tape, files_on_disk = download_data_from_nla(url)

        # Create output directory
        create_output_dir(OUTPUT_DIR, BATCH_DIR)

        print("Writing file list batches.")
        # Split files_on_tape into chunks of 10k and write to disk

        for i,files in enumerate(chunk_dict(files_on_tape,2000)):
            with open(os.path.join(BATCH_DIR,"on_tape","on_tape_batch_{}.json".format(i)),'w') as writer:
                # writer.writelines([x + "\n" for x in files])
                json.dump(files, writer)

        for i,files in enumerate(chunk_dict(files_on_disk,2000)):
            with open(os.path.join(BATCH_DIR,"on_disk","on_disk_batch_{}.json".format(i)),'w') as writer:
                # writer.writelines([x+"\n" for x in files])
                json.dump(files,writer)

    # Create lotus jobs for files on tape
    for i,file in enumerate(os.listdir(os.path.join(BATCH_DIR,"on_tape"))):
        if args['--index-docs']:
            cmd = "nla_sync_lotus_task.py -i {index} -f {input_file} -o {output_dir} --on-tape --index-docs".format(
                index=index, input_file=os.path.join(BATCH_DIR,"on_tape", file), output_dir=OUTPUT_DIR
            )
        else:
            cmd = "nla_sync_lotus_task.py -i {index} -f {input_file} -o {output_dir} --on-tape".format(
                index=index, input_file=os.path.join(BATCH_DIR,"on_tape", file), output_dir=OUTPUT_DIR
            )

        subprocess.call("bsub -q short-serial -W 24:00 {}".format(cmd),shell=True)
        if i> 0 and i % 10 == 0:
            print ("Waiting before submitting new jobs")
            sleep(40)

    # Create lotus jobs for files on disk
    for file in os.listdir(os.path.join(BATCH_DIR,"on_disk")):
        if args['--index-docs']:
            cmd = "nla_sync_lotus_task.py -i {index} -f {input_file} -o {output_dir} --on-disk --index-docs".format(
                index=index, input_file=os.path.join(BATCH_DIR,"on_disk", file), output_dir=OUTPUT_DIR
            )
        else:
            cmd = "nla_sync_lotus_task.py -i {index} -f {input_file} -o {output_dir} --on-disk".format(
                index=index, input_file=os.path.join(BATCH_DIR,"on_disk", file), output_dir=OUTPUT_DIR
            )

        print cmd
        subprocess.call("bsub -q short-serial -W 24:00 {}".format(cmd),shell=True)

if __name__ == "__main__":

    main()
