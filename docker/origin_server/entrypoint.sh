#!/bin/bash

. /usr/local/bin/pyutils.sh

. /src/venv/bin/activate

setup_pip

cd /src/swh-provenance || exit
python swh/provenance/tools/origins/server.py /origins.csv
