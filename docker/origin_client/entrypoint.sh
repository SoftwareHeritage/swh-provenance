#!/bin/bash

. /usr/local/bin/pyutils.sh

. /src/venv/bin/activate

setup_pip

cd /src/swh-provenance
python swh/provenance/tools/origins/client.py ${NB_CLIENTS}
