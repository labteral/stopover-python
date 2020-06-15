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


class Stopover:
    LISTEN_INTERVAL = 0.1

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self._uid = get_uid()
        self.session = requests.Session()

    @property
    def uid(self):
        return self._uid

    def put(self, value, stream: str, key: str = None) -> dict:
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

    def get(self, stream: str = None, receiver_group: str = None, instance: str = None) -> Message:
        instance = instance if instance else self.uid
        self._check_get_input(stream, receiver_group)
        message = self._get(stream, receiver_group, instance)
        return message

    def listen(self, stream: str = None, receiver_group: str = None, instance: str = None) -> Message:
        instance = instance if instance else self.uid
        self._check_get_input(stream, receiver_group)
        while True:
            message = self._get(stream, receiver_group, instance)

            if message.status != 'ok':
                time.sleep(Stopover.LISTEN_INTERVAL)
                continue

            yield message

    def commit(self, message: Message, receiver_group: str):
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

    def _check_get_input(self, stream: str, receiver_group: str):
        if receiver_group is None:
            raise ValueError('receiver group was not provided')

        if stream is None:
            raise ValueError('stream was not provided')

    def _get(self, stream: str, receiver_group: str, instance: str) -> Message:
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