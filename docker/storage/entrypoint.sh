#!/bin/bash

. /usr/local/bin/pyutils.sh

. /src/venv/bin/activate

setup_pip

cd /src/swh-provenance || exit

cat <<EOF > /tmp/start_storage.py
import swh.provenance.api.server
import os

# the following methods uses environment variable SWH_CONFIG_FILENAME
# to retrieve the configuration file
server = swh.provenance.api.server.make_server_from_configfile()

server.start()
while True:
    try:
        command = input("Enter EXIT to stop service: ")
        if command.lower() == "exit":
            break
    except KeyboardInterrupt:
        pass
server.stop()
EOF

python /tmp/start_storage.py
