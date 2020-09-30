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

    @property
    def dict(self):
        return {
            'stream': self.stream,
            'partition': self.partition,
            'index': self.index,
            'value': self.value,
            'timestamp': self.timestamp,
            'status': self.status
        }

    def __str__(self):
        message_dict = self.dict
        message_dict['value'] = str(message_dict['value'])
        return json.dumps(message_dict, indent=2)

    def copy(self):
        return Message(**self.dict)


class Stopover:
    LISTEN_INTERVAL = 0.1

    def __init__(self, endpoint: str, uid: str = None):
        self.endpoint = endpoint
        if uid is not None:
            self._uid = uid
        else:
            self._uid = get_uid()

        self.session = requests.Session()

    @property
    def uid(self):
        return self._uid

    def put(self, value, stream: str, key: str = None) -> dict:
        if not isinstance(value, (str, int, float, bytes, bool)):
            value = pickle.dumps(value, protocol=5)

        data = {
            'method': 'put_message',
            'params': {
                'value': value,
                'stream': stream,
            }
        }

        if key is not None:
            data['params']['key'] = key

        response = self.session.post(self.endpoint, data=compress(pack(data))).json()

        if 'status' not in response or response['status'] != 'ok':
            raise PutError(json.dumps(response))

    def get(self, stream: str = None, receiver_group: str = None, receiver: str = None) -> Message:
        receiver = receiver if receiver else self.uid
        self._check_get_input(stream, receiver_group)
        message = self._get(stream, receiver_group, receiver)
        return message

    def listen(self,
               stream: str = None,
               receiver_group: str = None,
               receiver: str = None) -> Message:
        receiver = receiver if receiver else self.uid
        self._check_get_input(stream, receiver_group)
        while True:
            message = self._get(stream, receiver_group, receiver)
            if not message or message.status != 'ok':
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
                'receiver_group': receiver_group,
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

    def _get(self, stream: str, receiver_group: str, receiver: str) -> Message:
        data = json.dumps({
            'method': 'get_message',
            'params': {
                'stream': stream,
                'receiver_group': receiver_group,
                'receiver': receiver
            }
        })
        response = self.session.post(self.endpoint, data=data)
        response_dict = unpack(decompress(response.content))

        if 'status' not in response_dict or response_dict['status'] != 'ok':
            return

        if isinstance(response_dict['value'], bytes):
            try:
                response_dict['value'] = pickle.loads(response_dict['value'])
            except Exception:
                pass
        return Message(**response_dict)