# Stopover Python SDK image.
#
# Replaces the brunneis/python:3.9 base (Python 3.9 is EOL and the tag carried
# no digest) with a digest-pinned Alpine image, and drops the 2020-2021 pins
# (msgpack 1.0.2, python-snappy 0.5.4, requests 2.24.0).
#
# python-snappy now uses wheels that bundle the C library, so
# libsnappy-dev and the build-essential install/purge dance are gone. The SDK
# setup.py reads its version without importing runtime dependencies.

FROM python:3.14.8-alpine3.24@sha256:8acac70227ce3b34da9453120c375cc5b66cd0b062d4dc6bc74286f81a3819e1

WORKDIR /opt/stopover-python
COPY src .
RUN \
    pip install --no-cache-dir \
    "msgpack==1.2.3" \
    "python-snappy==0.7.3" \
    "requests==2.34.2" \
    "setuptools==84.0.0" \
    "wheel==0.48.0" \
    && pip install --no-cache-dir --no-compile --no-build-isolation . \
    && rm -rf /root/.cache/pip \
    && find /usr/local -type d -name __pycache__ -exec rm -r {} \+ \
    && rm -rf /usr/local/lib/python3.14/site-packages/wheel* \
    /usr/local/lib/python3.14/site-packages/pip \
    /usr/local/lib/python3.14/site-packages/pip-*.dist-info \
    /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14 /usr/local/bin/wheel
