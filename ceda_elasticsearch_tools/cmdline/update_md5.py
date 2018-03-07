"""
Update MD5. Use this script to download all elasticsearch records in a given index that do not have an MD5 checksum
registered. This is then checked against the spot logs and available MD5s are added via the Elasticsearch Bulk API.

Use the -c | --calculate flag in order to bypass the log files and just calculate the md5s.
If restarting the script after a premature termination, use the --no-create-files flag to skip the elasticsearch query
phase.

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
    -c  --calculate     Calculate the MD5s from scratch and ignore the log files when calculating MD5
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
    """
    Setup logging to catch errors.
    :param log_dir: Directory to place the log in.
    :return: Logger object
    """
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
    """
    Wrapper to fire off script to read and process spot logs.
    :param arguments:
    :param index:
    :param log_dir:
    :return: None
    """

    # Download list of current spots
    spots = log_reader.SpotMapping()

    # Try to load completed spots
    try:
        with open('completed_spots.txt') as reader:
            completed_spots = reader.read().splitlines()

    except IOError:
        # If file not found create empty list
        completed_spots = []

    with open('completed_spots.txt', 'a') as comp_spot_output:

        for spot in spots:
            # Only run files not found in the list of completed spots
            if spot not in completed_spots:
                subprocess.call('md5.py -i {index} -o {log_dir} -s {spot} -a {archive_root}'.format(
                    spot=spot,
                    archive_root=spots.get_archive_root(spot),
                    index=index,
                    log_dir=log_dir),
                    shell=True)

                # Add completed spot to the completed spot file
                comp_spot_output.write("{}\n".format(spot))


def extract_id(page):
    """
    Extract the elasticsearch document id from the elasticsearch response
    :param page: Elasticsearch JSON response object
    :return: list of strings with document id and filepath
    """
    page_data = []
    for doc in page['hits']['hits']:
        doc_info = doc['_source']['info']
        id =  doc['_id']
        fpath = os.path.join(doc_info['directory'],doc_info['name'])
        page_data.append('{id},{filepath}\n'.format(id=id,filepath=fpath))
    return page_data


def write_page_to_file(page, page_no, output_dir):
    """
    Wrapper to write scroll of Elasticsearch to file to be processed later.
    :param page: Elasticsearch response to write to file
    :param page_no: Progress through the scroll
    :param output_dir: Directory to place the written page file
    :return: None
    """

    # Create the output directory if it does not exist
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    # Write and elasticsearch page to file
    with open(os.path.join(output_dir,"md5_update_page_{}.txt".format(page_no)),'w') as writer:
        writer.writelines(extract_id(page))


def download_files_missing_md5(index,host,port, output_dir):
    """
    Download all the records which do not have and MD5 checksum in the specified index
    :param index: Index to extract dat from
    :param host: host to target. Default: jasmin-es1.ceda.ac.uk
    :param port: port to access host. Default: 9200
    :param output_dir: Directory to write elasticsearch pages to
    :return: None
    """

    # Number of documents to get in one pass. 10,000 is the maximum.
    scroll_window = 10000

    # Query to select all valid files.
    query = json.dumps({
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "info.md5": {
                            "value": ""
                        }
                    }}
                ],
                "must_not": [
                    {"term": {
                        "info.location": {
                            "value": "on_tape"
                        }
                    }}
                ]
            }
        }
    })

    # Initialise Elasticsearch Updater object
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

    # Initialise progress bar
    pb = ProgressBar(max_pages)

    write_page_to_file(page, page_no, output_dir)

    # Start scrolling
    while (scroll_size > 0):
        page = esu.es.scroll(scroll_id=sid, scroll='2m')

        # Update the scroll ID
        sid = page['_scroll_id']

        # Update page_no
        page_no += 1

        # Update the progress bar
        pb.running(page_no)

        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])

        # Write the obtained page the file
        write_page_to_file(page, page_no, output_dir)

    # Terminate the progressbar
    pb.complete()


def calculate_md5s(arguments):
    """
    Wrapper to fire script to create the MD5s from scratch after processing the spot logs as best we can
    :param arguments: Commandline arguments
    :return: None
    """

    # Set config values from command line arguments
    index = arguments["INDEX"]
    log_dir = arguments["OUTPUT"]
    host = arguments["HOSTNAME"]
    port = arguments["PORT"]

    # Directory name to write the elasticsearch response
    document_output = 'page_files'

    # If flag is not in command line arguments write records to disk. This is here so that if the script needs to be
    # restarted, you can skip the elasticsearch query phase.
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

    # Set defaults if not supplied
    if arguments["HOSTNAME"] is None:
        arguments["HOSTNAME"] = "jasmin-es1.ceda.ac.uk"

    if arguments["PORT"] is None:
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
