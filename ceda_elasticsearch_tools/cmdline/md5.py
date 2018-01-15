
"""
Update MD5. Use this script to scan a spot logfile, gather the md5 checksums, query Elasticsearch to see if md5 matches
and if not, update it using the Elasticsearch Bulk API.

Usage:
    update_md5.py -h | --help
    update_md5.py --version
    update_md5.py
                   (-s SPOT         | --spot   SPOT        )
                   (-a ARCHIVE_ROOT         | --archive_root   ARCHIVE_ROOT        )
                   (-i INDEX        | --index   INDEX      )
                   (-o OUTPUT                              )
                   [-h HOSTNAME     | --hostname HOSTNAME  ]
                   [-p PORT         | --port    PORT       ]


Options:
    --help              Display help
    --version           Show Version
    -s  --spot          Spot name
    -i  --index         Elasticsearch index to test
    -o                  Logging output directory.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -p  --port          Elasticsearch read/write port [default: 9200]

"""
from docopt import docopt
from ceda_elasticsearch_tools.core import updater
from datetime import datetime
import os, logging
from ceda_elasticsearch_tools.cmdline import __version__

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
    spot = arguments["SPOT"]
    archive_root = arguments["ARCHIVE_ROOT"]

    # Set defaults
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

    # Initialise the elasticsearch updater instance
    update = updater.ElasticsearchUpdater(index=index, host=host, port=port)

    logger.info('Analysing {}'.format(spot))

    # Update MD5s for given spot
    start = datetime.now()
    update.update_md5(spot, archive_root)


    logger.info("Spot: {}  Update took: {}".format(spot, datetime.now() - start))


if __name__ == "__main__":
    main()
