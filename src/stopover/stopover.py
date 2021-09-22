#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utils import get_uid, compress, decompress, pack, unpack
from . import errors
import pickle
import requests
import time
import random


class STATUS:
    OK = 20
    ERROR = 50
    END_OF_STREAM = 21
    ALL_PARTITIONS_ASSIGNED = 22


class Response(dict):
    def __init__(self, *args, **kwargs):
        super(Response, self).__init__(*args, **kwargs)
        self.__dict__ = self


def raise_connection_error(method):
    def raise_connection_error_(
        self,
        *args,
        **kwargs,
    ):
        raise_connection_error = False
        try:
            return method(self, *args, **kwargs)
        except requests.exceptions.RequestException:
            raise_connection_error = True

        if raise_connection_error:
            raise errors.ServerConnectionError(
                f"cannot connect to the Stopover server ({self.endpoint})"
            )

    return raise_connection_error_


class Stopover:
    LISTEN_INTERVAL = 0.1

    def __init__(
        self,
        endpoint: str,
        uid: str = None,
    ):
        self.endpoint = endpoint
        if uid is not None:
            self._uid = uid
        else:
            self._uid = get_uid()

        self.temporary_offsets = {}
        self.session = requests.Session()

    @property
    def uid(self):
        return self._uid

    def put(
        self,
        value,
        stream: str,
        key: str = None,
    ) -> dict:
        if not isinstance(value, (str, int, float, bytes, bool)):
            value = pickle.dumps(value, protocol=5)

        response = self._put_call(value, stream, key=key)
        if response.status not in [STATUS.OK, 'OK', 'ok']:
            raise errors.PutError(response.status)
        return response

    @raise_connection_error
    def get(
        self,
        stream: str,
        receiver_group: str,
        receiver: str = None,
        partition: int = None,
        index: int = None,
        auto_commit: bool = None,
        progress_without_commit: bool = None,
    ) -> Response:
        response = self._get(
            stream,
            receiver_group,
            receiver=receiver,
            partition=partition,
            index=index,
            auto_commit=auto_commit,
            progress_without_commit=progress_without_commit,
        )
        if response.status not in [STATUS.OK, 'OK', 'ok']:
            raise errors.GetError(response.status)
        return response

    @raise_connection_error
    def listen(
        self,
        stream: str,
        receiver_group: str,
        receiver: str = None,
        partition: int = None,
        auto_commit: bool = None,
        progress_without_commit: bool = None,
    ) -> Response:
        receiver = receiver if receiver else self.uid
        while True:
            response = self._get(
                stream,
                receiver_group,
                receiver=receiver,
                partition=partition,
                auto_commit=auto_commit,
                progress_without_commit=progress_without_commit,
            )
            if response.status not in [STATUS.OK, 'OK', 'ok']:
                time.sleep(Stopover.LISTEN_INTERVAL)
                continue
            yield response

    def commit(
        self,
        message: Response,
        receiver_group: str,
    ):
        response = self._commit_call(message, receiver_group)
        if response.status not in [STATUS.OK, 'OK', 'ok']:
            raise errors.CommitError(response.status)
        return response

    def knock(
        self,
        receiver_group: str,
        receiver: str = None,
    ):
        response = self._knock_call(receiver_group, receiver=receiver)
        if response.status not in [STATUS.OK, 'OK', 'ok']:
            raise errors.KnockError(response.status)
        return response

    def _get(
        self,
        stream: str,
        receiver_group: str,
        receiver: str = None,
        partition: int = None,
        index: int = None,
        auto_commit: bool = None,
        progress_without_commit: bool = None,
    ) -> Response:
        receiver = receiver if receiver else self.uid

        if auto_commit is None:
            auto_commit = False
        if progress_without_commit is None:
            progress_without_commit = False

        if progress_without_commit:
            partition, index = self._get_next_partition_and_index(
                stream,
                receiver_group,
                receiver,
            )

        response = self._get_call(
            stream,
            receiver_group,
            receiver,
            partition,
            index,
        )

        if response.status in [STATUS.OK, 'OK', 'ok']:
            if isinstance(response.value, bytes):
                try:
                    response.value = pickle.loads(response.value)
                except Exception:
                    pass

            if progress_without_commit:
                self._increment_partition_index(response)

            if auto_commit:
                self.commit(response, receiver_group)

        return response

    @raise_connection_error
    def _put_call(
        self,
        value: bytes,
        stream: str,
        key: str = None,
    ) -> dict:
        data = {
            'method': 'put_message',
            'params': {
                'value': value,
                'stream': stream,
            }
        }
        if key is not None:
            data['params']['key'] = key
        response = self.session.post(self.endpoint, data=compress(pack(data)))
        return Response(**unpack(decompress(response.content)))

    def _get_next_partition_and_index(
        self,
        stream: str,
        receiver_group: str,
        receiver: str,
    ):
        partition = None
        index = None

        if stream not in self.temporary_offsets:
            self.temporary_offsets[stream] = {}
        if receiver_group not in self.temporary_offsets[stream]:
            self.temporary_offsets[stream][receiver_group] = {}
        if receiver not in self.temporary_offsets[stream][receiver_group]:
            self.temporary_offsets[stream][receiver_group][receiver] = {}

        temporary_offsets = self.temporary_offsets[stream][receiver_group][
            receiver]

        assigned_partitions = list(temporary_offsets.keys())
        if assigned_partitions:
            partition = assigned_partitions[random.randint(
                0,
                len(assigned_partitions) - 1,
            )]

            index = (
                None if partition not in temporary_offsets else
                temporary_offsets[partition]
            )

        return partition, index

    def _increment_partition_index(self, response):
        temporary_offsets = self.temporary_offsets[response.stream][
            response.receiver_group][response.receiver]
        temporary_offsets[response.partition] = (response.index + 1)
        for assigned_partition in response.assigned_partitions:
            if assigned_partition not in response.assigned_partitions:
                del temporary_offsets[assigned_partition]
        for assigned_partition in response.assigned_partitions:
            if assigned_partition not in temporary_offsets:
                temporary_offsets[assigned_partition] = None

    @raise_connection_error
    def _get_call(
        self,
        stream: str,
        receiver_group: str,
        receiver: str,
        partition: int,
        index: int,
    ) -> Response:
        data = {
            'method': 'get_message',
            'params': {
                'stream': stream,
                'receiver_group': receiver_group,
                'receiver': receiver,
                'index': index
            }
        }
        response = self.session.post(self.endpoint, data=compress(pack(data)))
        return Response(**unpack(decompress(response.content)))

    def _commit_call(
        self,
        message: Response,
        receiver_group: str,
    ):
        data = {
            'method': 'commit_message',
            'params': {
                'stream': message.stream,
                'partition': message.partition,
                'index': message.index,
                'receiver_group': receiver_group,
            }
        }

        response = self.session.post(self.endpoint, data=compress(pack(data)))
        return Response(**unpack(decompress(response.content)))

    @ raise_connection_error
    def _knock_call(
        self,
        receiver_group: str,
        receiver: str = None,
    ):
        receiver = receiver if receiver else self.uid
        data = {
            'method': 'knock',
            'params': {
                'receiver_group': receiver_group,
                'receiver': receiver
            }
        }

        response = self.session.post(self.endpoint, data=compress(pack(data)))
        return Response(**unpack(decompress(response.content)))
