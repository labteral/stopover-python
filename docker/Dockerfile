FROM brunneis/python:3.9

RUN \
    apt-get update \
    && apt-get -y install libsnappy-dev \
    && dpkg-query -Wf '${Package}\n' | sort > init_pkgs \
    && apt-get -y install build-essential \
    && dpkg-query -Wf '${Package}\n' | sort > current_pkgs \
    && pip install --upgrade pip \
    && pip install \
    "msgpack==1.0.2" \
    "python-snappy==0.5.4" \
    "requests==2.24.0" \
    && apt-get -y purge $(diff -u init_pkgs current_pkgs | grep -E "^\+" | cut -d + -f2- | sed -n '1!p' | uniq) \
    && apt-get clean
WORKDIR /opt/stopover-python
ADD src .
RUN \
    cd /opt/stopover-python \
    && python setup.py install \
    && rm -rf init_pkgs current_pkgs /root/.cache/pip \
    && find / -type d -name __pycache__ -exec rm -r {} \+
