"""
Feed in a list of file paths.

Usage:
    spot_checker.py --help
    spot_checker.py --version
    spot_checker.py
                   (-f FILE         | --file FILE     )
                   (-o OUTPUT       | --output OUTPUT )
                   (-i INDEX        | --index   INDEX )
                   [-h HOSTNAME     | --hostname HOSTNAME ]
                   [-b BLOCKSIZE    | --blocksize BLOCKSIZE ]


Options:
    --help              Display help.
    --version           Show Version.
    -f  --file          Input file.
    -o  --output        Logging output directory.
    -i  --index         Elasticsearch index to test.
    -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
    -b  --blocksize     Number of files to chunk into bulk query [default: 800]
"""
from docopt import docopt


import os
import simplejson as json
from elasticsearch import Elasticsearch
from ceda_elasticsearch_tools.cmdline import __version__


def es_connection(host="jasmin-es1.ceda.ac.uk", port=9200):
    conn = Elasticsearch(hosts=[{"host": host, "port": port}])
    return conn

def make_query(test_list, blocksize=800):

    msearch_json = ""
    query_list = []

    for i, case in enumerate(test_list, 1):

        dir = os.path.dirname(case)
        filename = os.path.basename(case).strip()
        index = json.dumps({}) + "\n"
        query = json.dumps({
                                "query": {
                                    "bool": {
                                        "must": [
                                           {"match": {
                                              "info.name": filename
                                           }}
                                        ],
                                        "filter":{
                                            "term": {
                                               "info.directory": dir
                                            }
                                        }
                                    }
                                }
                            })

        msearch_json += index + query + "\n"

        if i % blocksize == 0:
            query_list.append(msearch_json)
            msearch_json = ""

    if msearch_json:
        query_list.append(msearch_json)

    return query_list

def process_list(es_connection, file_list, query_list, config):
    blocksize =  config["BLOCKSIZE"]

    total_files = len(file_list)
    total_in = 0
    total_out = 0
    error_list = []

    scroll_count = 0

    for mquery in query_list:
        results = es_connection.msearch(index="ceda-level-1", body=mquery, request_timeout=60 )

        for i, response in enumerate(results["responses"]):
            if response["hits"]["total"] > 0:
                total_in += 1
            else:
                error_list.append(file_list[i + (blocksize * scroll_count)])
                total_out += 1

        scroll_count += 1
    if total_files > 0:
        percent_missing = (total_out/float(total_files))*100
    else:
        percent_missing = 0

    # Send output to file
    output_dir = config["OUTPUT"]
    output_name = os.path.basename(config["FILE"]).split(".txt")[0] + "_log.txt"

    with open(os.path.join(output_dir,output_name),'w') as output:
        output.write("Summary: Total Files in Spot: {} Total Indexed: {} Total Missing: {} Percentage Missing: {:.2f}% \n".format(total_files, total_in, total_out, percent_missing))
        output.writelines(error_list)


def get_args(config):

    if not config["HOSTNAME"]:
        config["HOSTNAME"] = "jasmin-es1.ceda.ac.uk"

    if not config["BLOCKSIZE"]:
        config["BLOCKSIZE"] = 800

    config["PORT"] = 9200

    return config

def dir_exists(dir):
    """
    Tests if a dir exists
    :param dir: dirname to test
    :return: Boolean
    """
    try:
        os.stat(dir)
    except OSError:
        return False
    else:
        return True


def main():
    config = get_args(docopt(__doc__, version=__version__))

    # Create output dir
    if not dir_exists(config["OUTPUT"]):
        os.mkdir(config["OUTPUT"])

    # Open connection with elasticsearch
    es_conn = es_connection(config["HOSTNAME"],config["PORT"])

    with open(config["FILE"],"r") as input_file:
        filelist = input_file.readlines()
        query_list = make_query(filelist, config["BLOCKSIZE"])

    output_name = os.path.basename(config["FILE"]).split('.txt')[0]
    process_list(es_conn, filelist, query_list, config)





if __name__== "__main__":
    main()