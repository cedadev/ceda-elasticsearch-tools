"""
Update MD5. Use this script to scan a spot logfile, gather the md5 checksums, query Elasticsearch to see if md5 matches
and if not, update it using the Elasticsearch Bulk API.
Use the -c | --calculate flag in order to bypass the log files and just calculate the md5s.

Usage:
    update_md5.py -h | --help
    update_md5.py --version
    update_md5.py
                   (-i INDEX        | --index   INDEX       )
                   (-o OUTPUT                               )
                   [-h HOSTNAME     | --hostname HOSTNAME   ]
                   [-p PORT         | --port    PORT        ]
                   [-c              | --calculate           ]
                   [--no-create-files                       ]


Options:
    --help              Display help
    --version           Show Version
    -i  --index         Elasticsearch index to test
    -o                  Logging output directory.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -p  --port          Elasticsearch read/write port [default: 9200]
    -c  --calculate     Calculate the MD5s from scratch and ignore the log files
    --no-create-files   Don't repeat the elasticsearch download phase

"""
from docopt import docopt
from ceda_elasticsearch_tools.core import log_reader
from datetime import datetime
import os, logging
from ceda_elasticsearch_tools.cmdline import __version__
import subprocess
import simplejson as json
from ceda_elasticsearch_tools.core.updater import ElasticsearchUpdater
import math
from ceda_elasticsearch_tools.core.util import ProgressBar


def logger_setup(log_dir):
    FORMAT = "%(levelname)s %(asctime)-15s %(message)s"

    logging_path = log_dir

    # Make sure the logger path exists
    if not os.path.exists(logging_path):
        os.makedirs(logging_path)

    # initiate log
    log_name = "md5_update-" + datetime.now().strftime("%Y-%m-%d") + ".log"
    logging.basicConfig(filename=os.path.join(logging_path, log_name), level=logging.INFO, format=FORMAT)

    return logging.getLogger(__name__)


def update_from_logs(arguments, index, log_dir):

    # Download list of current spots
    spots = log_reader.SpotMapping()

    # Load completed spots
    try:
        with open('completed_spots.txt') as reader:
            completed_spots = reader.read().splitlines()

    except IOError:
        completed_spots = []

    with open('completed_spots.txt', 'a') as comp_spot_output:

        for spot in spots:
            # Only run non-complete spots
            if spot not in completed_spots:
                subprocess.call('md5.py -s {spot} -a {archive_root} -i {index} -o {log_dir}'.format(
                    spot=spot,
                    archive_root=spots.get_archive_root(spot),
                    index=index,
                    log_dir=log_dir),
                    shell=True)

                # Add completed spot to the completed spot file
                comp_spot_output.write("{}\n".format(spot))


def extract_id(page):
    page_data = []
    for doc in page['hits']['hits']:
        doc_info = doc['_source']['info']
        id =  doc['_id']
        fpath = os.path.join(doc_info['directory'],doc_info['name'])
        page_data.append('{},{}\n'.format(id,fpath))
    return page_data


def write_page_to_file(page, page_no, output_dir):

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    with open(os.path.join(output_dir,"md5_update_page_{}.txt".format(page_no)),'w') as writer:
        writer.writelines(extract_id(page))

def download_files_missing_md5(index,host,port, output_dir):

    scroll_window = 10000

    query = json.dumps({
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "info.md5": {
                            "value": ""
                        }
                    }}
                ]
            }
        }
    })


    esu = ElasticsearchUpdater(index, host, port)

    page_no = 1
    # Initialize the scroll
    page = esu.es.search(
        index=esu.index,
        scroll='2m',
        size=scroll_window,
        body=query)

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    max_pages = math.ceil(scroll_size/float(scroll_window))
    pb = ProgressBar(max_pages)
    write_page_to_file(page, page_no, output_dir)

    # Start scrolling
    while (scroll_size > 0):
        page = esu.es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Update page_no
        page_no += 1
        pb.running(page_no)

        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        # Do something with the obtained page
        write_page_to_file(page, page_no, output_dir)
    pb.complete()


def calculate_md5s(arguments):
    index = arguments["INDEX"]
    log_dir = arguments["OUTPUT"]
    host = arguments["HOSTNAME"]
    port = arguments["PORT"]

    document_output = 'page_files'

    if arguments["--no-create-files"] == False:
        print "Downloading records missing MD5s"
        # Download ES id and filepath to local file.
        download_files_missing_md5(index, host, port, document_output)

    # Submit those files as jobs to lotus
    print "Submit jobs to lotus"
    for file in os.listdir(document_output):
        command = 'md5.py -i {index} -o {log_dir} --pagefile {page}'.format(
            index=index,
            log_dir=log_dir,
            page=file)

        subprocess.call("bsub -q short-serial -W 24:00 {}".format(command),shell=True)



def main():
    arguments = docopt(__doc__, version=__version__)

    # Extract commandline args.
    index = arguments["INDEX"]
    log_dir = arguments["OUTPUT"]

    if arguments["HOSTNAME"] == None:
        arguments["HOSTNAME"] = "jasmin-es1.ceda.ac.uk"


    if arguments["PORT"] == None:
        arguments["PORT"] = 9200

    # setup logging
    logger = logger_setup(log_dir)

    logger.info("Updating {} index with md5 checksums.".format(index))

    # Start time
    begin = datetime.now()

    if arguments["-c"] or arguments["--calculate"]:
        print "Updating MD5s by calculation"
        calculate_md5s(arguments)
    else:
        print "Updating MD5 from spot logs"
        update_from_logs(arguments, index, log_dir)



    logger.info("Whole operation took: %s" % (datetime.now() - begin))


if __name__ == "__main__":
    main()
