"""
Submits jobs to run on lotus, monitors progress and prints a summary table at the end.

Usage:
    download_index.py --help
    download_index.py --version
    download_index.py
                   (-i INDEX        | --index INDEX )
                   (-o OUTPUT       | --output OUTPUT )
                   [-m              | --mapping ]
                   [-s SIZE         | --size SIZE ]
                   [-a API_KEY      | --api-key API_KEY ]


Options:
    --help              Display help.
    --version           Show Version.
    -i  --index         Elasticsearch index to download.
    -o  --output        Script output directory.
    -m  --mapping       Flag if mapping should be saved.
    -h  --hostname      Elasticsearch host to query [default: elasticsearch.ceda.ac.uk]
    -s  --size          Number of records per query [default: 10000]
    -a  --api-key       API key for Elasticsearch.
"""

import json
import os

from docopt import docopt

from ceda_elasticsearch_tools.cmdline import __version__
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient


def search(es, config, search_after):
    query = {
        "sort": [
            {"_id": "asc"},
        ],
        "size": config["SIZE"] if config["SIZE"] else 10000,
    }
    if search_after:
        query["search_after"] = search_after

    return es.search(
        index=config["INDEX"],
        size=config["SIZE"],
        body=query,
        request_timeout=900,
    )


def dump(path, data):
    with open(
        file=path,
        mode="w+",
        encoding="utf-8",
    ) as file:
        json.dump(data, file)


def get_mapping(es, config):
    mapping = es.indices.get_mapping(index=config["INDEX"])

    mapping_path = os.path.join(config["OUTPUT"], "mapping.json")

    dump(mapping, mapping_path)

    print("Mapping saved")


def records(es, config):
    search_after = []

    record_count = 0
    file_count = 0

    while True:
        result = search(es, config, search_after)

        if not result["hits"]["hits"]:
            break

        for record in result["hits"]["hits"]:
            record_count += 1
            file_path = os.path.join(config["OUTPUT"], record["_id"])
            dump(record, file_path)
            file_count += 1

        search_after = [result["hits"]["hits"][-1]["_id"]]

    print(
        f"Total Records: {record_count} Total Files created: {file_count} Total Missed: {record_count - file_count} Percent Missing: {((record_count - file_count)/record_count)*100}%"
    )


def main():
    # Parse the command line arguments
    config = docopt(__doc__, version=__version__)

    es = CEDAElasticsearchClient(headers={"x-api-key": config["API_KEY"]})

    if config["MAPPING"]:
        get_mapping(es, config)

    records(es, config)


if __name__ == "__main__":

    main()
