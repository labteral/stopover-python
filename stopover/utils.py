#!/usr/bin/env python
# -*- coding: utf-8 -*-

import bson
import snappy
import json
import time
import random
import hashlib
from uuid import uuid4


def pack(message: dict) -> bytes:
    return bson.encode(message)


def unpack(message: bytes) -> dict:
    try:
        return bson.decode(message)
    except bson.errors.InvalidBSON:
        return json.loads(message)


def compress(message: bytes) -> bytes:
    return snappy.compress(message)


def decompress(message: bytes) -> bytes:
    try:
        return snappy.decompress(message)
    except snappy.UncompressError:
        return message


def get_timestamp_ms() -> int:
    return int(round(time.time() * 1000))


def string_to_sha3_256(text: str):
    return hashlib.sha3_256(text.encode('utf-8')).hexdigest()


def get_uid():
    return string_to_sha3_256(str(uuid4()))[:12]