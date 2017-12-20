
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
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -p  --port          Elasticsearch read/write port [default: 9200]
    -o                  Logging output directory.

"""
from docopt import docopt
from ceda_elasticsearch_tools.core import log_reader
from ceda_elasticsearch_tools.core import updater
from datetime import datetime
import os, logging
from ceda_elasticsearch_tools.cmdline import __version__
from tqdm import tqdm

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

    print "Updating MD5 checksums for records in {} spots".format(len(spots))
    logger.info("Updating {} index with md5 checksums.".format(index))

    update = updater.ElasticsearchUpdater(index=index, host=host, port=port)

    begin = datetime.now()
    for spot in tqdm(spots):
        logger.info('Analysing {}'.format(spot))

        # start = datetime.now()
        spot_base = spots.get_archive_root(spot)
        update.update_md5(spot, spot_base)
        # update.get_md5_bulk_update_from_spotlog(spot_name=spot, spotmapobj=spots, log_base_dir=logging_path,
        #                                         update=True)
        # print "Spot: %s took: %s to analyse." % (spot, (datetime.now() - start))

    logger.info("Whole operation took: %s" % (datetime.now() - begin))

if __name__ == "__main__":
    main()
