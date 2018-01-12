"""
Update MD5. Use this script to scan a spot logfile, gather the md5 checksums, query Elasticsearch to see if md5 matches
and if not, update it using the Elasticsearch Bulk API.

Usage:
    update_md5.py -h | --help
    update_md5.py --version
    update_md5.py
                   (-i INDEX        | --index   INDEX )
                   (-o OUTPUT                           )
                   [-h HOSTNAME     | --hostname HOSTNAME ]
                   [-p PORT         | --port    PORT ]


Options:
    --help              Display help
    --version           Show Version
    -i  --index         Elasticsearch index to test
    -o                  Logging output directory.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -p  --port          Elasticsearch read/write port [default: 9200]

"""
from docopt import docopt
from ceda_elasticsearch_tools.core import log_reader
from ceda_elasticsearch_tools.core import updater
from datetime import datetime
import os, logging
from ceda_elasticsearch_tools.cmdline import __version__
from tqdm import tqdm
import subprocess


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


def main():
    arguments = docopt(__doc__, version=__version__)

    # Extract commandline args.
    index = arguments["INDEX"]
    log_dir = arguments["OUTPUT"]

    if arguments["HOSTNAME"] == None:
        host = "jasmin-es1.ceda.ac.uk"
    else:
        host = arguments["HOSTNAME"]

    if arguments["PORT"] == None:
        port = 9200
    else:
        port = arguments["PORT"]

    # setup logging
    logger = logger_setup(log_dir)

    spots = log_reader.SpotMapping()

    logger.info("Updating {} index with md5 checksums.".format(index))

    # Load completed spots
    try:
        with open('completed_spots.txt') as reader:
            completed_spots = reader.read().splitlines()

    except IOError:
        completed_spots = []

    with open('completed_spots.txt', 'a') as comp_spot_output:

        begin = datetime.now()
        for spot in spots:
            # Only run non-complete spots
            if spot not in completed_spots:
                subprocess.call('md5.py -s {spot} -a {archive_root} -i {index} -o {log_dir}'.format(
                        spot=spot,
                        archive_root=spots.get_archive_root(
                            spot),
                        index=index,
                        log_dir=log_dir),
                    shell=True)

                # Add completed spot to the completed spot file
                comp_spot_output.write("{}\n".format(spot))

        logger.info("Whole operation took: %s" % (datetime.now() - begin))


if __name__ == "__main__":
    main()
