#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utils import get_uid, compress, decompress, pack, unpack
from .errors import PutError, GetError, CommitError
import pickle
import requests
import json
import time


class Message:
    def __init__(self, stream, partition, index, value, timestamp, status=None, **kwargs):
        self.stream = stream
        self.partition = partition
        self.index = index
        self.value = value
        self.timestamp = timestamp
        self.status = status


class Sender:
    def __init__(self, endpoint: str, stream: str):
        self.endpoint = endpoint
        self.stream = stream
        self.session = requests.Session()

    def put(self, value, stream: str = None, key: str = None) -> dict:
        stream = stream if stream else self.stream

        data = {
            'method': 'put_message',
            'params': {
                'value': pickle.dumps(value),
                'stream': stream,
            }
        }

        if key is not None:
            data['params']['key'] = key

        response = self.session.post(self.endpoint, data=compress(pack(data))).json()
        if response['status'] != 'ok':
            raise PutError


class Receiver:
    LISTEN_INTERVAL = 0.1

    def __init__(self, endpoint, stream=None, receiver_group=None, instance=None):
        self.endpoint = endpoint
        self.stream = stream
        self.receiver_group = receiver_group
        if instance is None:
            instance = get_uid()
        self.instance = instance
        self.session = requests.Session()

    def get(self, stream: str = None, receiver_group: str = None, instance: str = None) -> Message:
        stream, receiver_group, instance = self._process_get_input(stream, receiver_group, instance)
        message = self._get(stream, receiver_group, instance)
        return message

    def listen(self, stream: str = None, receiver_group: str = None, instance: str = None) -> Message:
        stream, receiver_group, instance = self._process_get_input(stream, receiver_group, instance)
        while True:
            message = self._get(stream, receiver_group, instance)

            if message.status != 'ok':
                time.sleep(Receiver.LISTEN_INTERVAL)
                continue

            yield message

    def commit(self, message, receiver_group: str = None):
        receiver_group = receiver_group if receiver_group else self.receiver_group

        data = json.dumps({
            'method': 'commit_message',
            'params': {
                'stream': message.stream,
                'partition': message.partition,
                'index': message.index,
                'receiver': receiver_group,
            }
        })

        response = self.session.post(self.endpoint, data=data).json()
        if response['status'] != 'ok':
            raise CommitError

    def _process_get_input(self, stream: str = None, receiver_group: str = None, instance: str = None):
        stream = stream if stream else self.stream
        receiver_group = receiver_group if receiver_group else self.receiver_group
        instance = instance if instance else self.instance

        if receiver_group is None:
            raise ValueError('stream was not provided')

        if stream is None:
            raise ValueError('stream was not provided')

        return stream, receiver_group, instance

    def _get(self, stream: str = None, receiver_group: str = None, instance: str = None) -> Message:
        data = json.dumps({
            'method': 'get_message',
            'params': {
                'stream': stream,
                'receiver': receiver_group,
                'instance': instance
            }
        })
        response = self.session.post(self.endpoint, data=data)
        message = Message(**unpack(decompress(response.content)))
        return message