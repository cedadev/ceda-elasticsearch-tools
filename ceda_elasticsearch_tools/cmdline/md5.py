
"""
Update MD5. Use this script to scan a spot logfile, gather the md5 checksums, query Elasticsearch to see if md5 matches
and if not, update it using the Elasticsearch Bulk API.

Usage:
    update_md5.py -h | --help
    update_md5.py --version
    update_md5.py
                   (-i INDEX                | --index   INDEX                   )
                   (-o OUTPUT               | --output OUTPUT                   )
                   [-s SPOT                 | --spot   SPOT                     ]
                   [-a ARCHIVE_ROOT         | --archive_root   ARCHIVE_ROOT     ]
                   [-h HOSTNAME             | --hostname HOSTNAME               ]
                   [-p PORT                 | --port    PORT                    ]
                   [--pagefile PAGE_FILE                                        ]


Options:
    --help              Display help
    --version           Show Version
    -i  --index         Elasticsearch index to test
    -o  --output        Logging output directory.
    -s  --spot          Spot name
    -a  --archive_root  Root path on the archive mapped to spot.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -p  --port          Elasticsearch read/write port [default: 9200]
    --pagefile          File containing id and path information

"""
from docopt import docopt
from ceda_elasticsearch_tools.core import updater
from datetime import datetime
import os, logging
from ceda_elasticsearch_tools.cmdline import __version__
import hashlib
import simplejson as json

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

def file_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname,'rb') as f:
        for chunk in iter(lambda: f.read(4096),b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def main():
    arguments = docopt(__doc__, version=__version__)

    # Extract commandline args.
    index = arguments["INDEX"]
    log_dir = arguments["OUTPUT"]
    spot = arguments["SPOT"]
    archive_root = arguments["ARCHIVE_ROOT"]
    pagefile = arguments["PAGE_FILE"]

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


    if pagefile == None:
        # Are processing log files by spot

        logger.info('Analysing {}'.format(spot))

        # Update MD5s for given spot
        start = datetime.now()
        update.update_md5(spot, archive_root)


        logger.info("Spot: {}  Update took: {}".format(spot, datetime.now() - start))

    else:
        # Are calculating MD5s from scratch.
        with open(os.path.join('page_files',pagefile)) as reader:
            lines = reader.readlines()

            # Check md5s
            update_total = 0
            md5_json = ""

            try:
                for file in lines:
                    line_split = file.split(',')

                    es_id = line_split[0].strip()
                    file_path = line_split[1].strip()

                    md5 = file_md5(file_path)

                    if md5:
                        update_total += 1

                        index = json.dumps({"update": {"_id": es_id, "_type": "file"}}) + "\n"
                        md5_field = json.dumps({"source": {"doc": {"info": {"md5": md5}}}}) + "\n"
                        md5_json += index + md5_field

                    if update_total > 800:
                        update.make_bulk_update(md5_json)
                        md5_json = ""
                        update_total = 0

                if md5_json:
                    update.make_bulk_update(md5_json)

            except Exception, msg:
                logger.error(msg)


if __name__ == "__main__":
    main()
