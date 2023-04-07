FROM python:3.9

COPY requirements* /
COPY docker/pyutils.sh /usr/local/bin

RUN apt-get update && apt-get -y install rsync libcmph-dev && \
    addgroup --gid 1000 swh && \
    useradd --gid 1000 --uid 1000 -m -d /src swh && \
    chmod a+x /usr/local/bin/pyutils.sh

USER swh

RUN python -m venv /src/venv && \
    . /src/venv/bin/activate && \
    python -m pip install --upgrade pip && \
    ls /requirements* | xargs -t -n1 pip install -r

ENTRYPOINT /entrypoint.sh

ENV SWH_CONFIG_FILENAME=/config.yml
