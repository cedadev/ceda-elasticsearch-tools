# ceda-elasticsearch-tools

General elasticsearch tools for use by CEDA in the many applications which will eventually talk with elasticsearch.
Central repository for update and search tools which make use of the bulk APIs such as msearch and bulk in order to increase
speed and limit API calls.

## Installation

    pip install git+https://github.com/cedadev/ceda-elasticsearch-tools.git

## Command Line Tools

### update_md5.py
Use spot log files as source of md5 checksums across the whole archive. Designed as a one-shot to be performed when
creating a new ceda archive level index. Maintenance to be performed by reading the deposit logs.

#### Usage
    update_md5.py -i INDEX -o LOG_DIR [-h HOST] [-p PORT ]
    
    options:
        --help              Display help
        --version           Show Version
        -i  --index         Elasticsearch index to test
        -o                  Logging output directory.
        -h  --hostname      Elasticsearch host to query [default: jasmin-es1.ceda.ac.uk]
        -p  --port          Elasticsearch read/write port [default: 9200]
        
`HOST` and `PORT` default to `jasmin-es1.ceda.ac.uk` and `9200`

### file_on_tape.py
Sets the location of all items which are currently on tape as defined by the NLA (near-line archive). Updates the
target index to be correct with NLA.
Designed as a one shot script to be used when creating a new ceda archive level index. Maintenance to be performed by
the NLA itself.

#### Usage

    file_on_tape.py <index> [--host HOST] [--port PORT]
    Options:
    -h --help   Show this screen.
    --version   Show version.
    --host      Elasticsearch host to target.
    --port      Elasticsearch port to target.
    
`HOST` and `PORT` default to `jasmin-es1.ceda.ac.uk` and `9200`
