"""
Generates a graph displaying coverage for each group

Usage:
    coverage_graphs --help
    coverage_graphs --version
    coverage_graphs (-i INDEX )( -n NAMESPACE )

Options:
    --help      Displays this message
    --version   Displays version number
    -i          The elasticsearch index to test
    -n          The namespace to test
"""
from docopt import docopt

import simplejson as json
from ceda_elasticsearch_tools.cmdline import __version__
import os
import re
from ceda_elasticsearch_tools.core.updater import  ElasticsearchUpdater, ElasticsearchQuery
from ceda_elasticsearch_tools.core import util



def main():
    base = os.path.dirname(__file__)

    # Load options
    with open(os.path.join(base,'../../config/config.json')) as config_file:
        config = json.load(config_file)
    opts = docopt(__doc__, version=__version__)

    # Initialise elasticsearch connection
    update = ElasticsearchUpdater(index=opts["INDEX"], host="jasmin-es1.ceda.ac.uk", port=9200)


    # Select namespace
    namespace = opts["NAMESPACE"]

    # Get query information based on namespace
    if namespace == "ceda-eo":
        params, query = ElasticsearchQuery.ceda_eo_manifest()
    else:
        params,query = None, None

    try:
        test_groups = config[namespace]
    except KeyError:
        print (f"{namespace} does not exist in config file ceda_elasticsearch_tools/config/config.json")
        exit()


    # Get all files in namespace
    for group in test_groups:
        dir_paths = test_groups[group]["files"]
        pattern = re.compile(test_groups[group]["metadata_pattern"])

        file_list = []
        # Check elasticsearch for files
        for dir in dir_paths:
            for root,_,files in os.walk(dir, followlinks=True):
                for file in files:
                    if pattern.match(file):
                        file_list.append(os.path.join(root,file))

        #
        # for file in file_list:
        #     print params(file)
        results = update.check_files_existence(params,query,file_list)

        print(f"Group: {group} Total files: {len(file_list)}"
               f" Files in: {len(results['True'])} Files out: {len(results['False'])}"
               f" Coverage: {util.percent(len(file_list),len(results['True']))}")





        # Output lists of missing files
        # Plot graph


if __name__ == "__main__":
    main()

