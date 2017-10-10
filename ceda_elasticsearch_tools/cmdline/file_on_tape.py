'''
file_on_tape.py

Updates the file location on the target elasticsearch index using the NLA as a baseline.
To be used as a command line tool.

Usage:
    file_on_tape.py INDEX [--host HOST] [--port PORT]
    file_on_tape.py --version

Options:
    -h --help   Show this screen.
    --version   Show version.
    --host      Elasticsearch host to target.
    --port      Elasticsearch port to target.
'''
from docopt import docopt

from ceda_elasticsearch_tools.core.updater import ElasticsearchUpdater, ElasticsearchQuery
import requests
import pkg_resources
from time import sleep
import itertools, sys
from multiprocessing import Process

def loading(message):
    for i in itertools.cycle(['|', '/', '-', '\\', '|', '/', '-', '\\']):
        text = message + ": {}".format(i)
        sys.stdout.write(text)
        sys.stdout.flush()
        sys.stdout.write('\r')
        sleep(0.1)

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

    print "Script settings. ElasticSearch index: {} host: {} port: {}".format(index, host, port)

    # Get list of files on tape
    url = "http://nla.ceda.ac.uk/nla_control/api/v1/files?stages=T"
    p = Process(target=loading,args=("Retrieving list of files on tape from nla.ceda.ac.uk",))
    p.start()

    r = requests.get(url).json()
    files = [x["path"] for x in r["files"]]

    p.terminate()

    print ""
    print "Number of files on tape: %s" % len(files)

    # Update documents
    print "Updating ElasticSearch"

    # Get file query for ceda_fbs
    params, query = ElasticsearchQuery.ceda_fbs()

    ElasticsearchUpdater(index=index,
                         host=host,
                         port=port
                         ).update_location(file_list=files, params=params, search_query=query, on_disk=False)

if __name__ == "__main__":

    main()

