#!/usr/bin/env python
# -*- coding: utf-8 -*-

import msgpack
import snappy
import time
import hashlib
from uuid import uuid4


def pack(message: dict) -> bytes:
    return msgpack.packb(message)


def unpack(message: bytes) -> dict:
    return msgpack.unpackb(message)


def compress(message: bytes) -> bytes:
    return snappy.compress(message)


def decompress(message: bytes) -> bytes:
    return snappy.decompress(message)


def get_timestamp_ms() -> int:
    return int(round(time.time() * 1000))


def string_to_sha3_256(text: str):
    return hashlib.sha3_256(text.encode('utf-8')).hexdigest()


def get_uid():
    return string_to_sha3_256(str(uuid4()))[:12]
