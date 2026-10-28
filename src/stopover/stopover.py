#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utils import get_uid, compress, decompress, pack, unpack
from . import errors
from .serialization import MessageUnpickler
import pickle
import requests
import time
import random
import base64
import math
from os import environ
from urllib.parse import urlsplit, urlunsplit


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
            try:
                endpoint = urlsplit(self.endpoint)
                endpoint = urlunsplit((
                    endpoint.scheme, endpoint.netloc.rsplit('@', 1)[-1],
                    endpoint.path, '', ''))
            except (TypeError, ValueError):
                endpoint = '<configured endpoint>'
            raise errors.ServerConnectionError(
                f"cannot connect to the Stopover server ({endpoint})"
            )

    return raise_connection_error_


class Stopover:
    LISTEN_INTERVAL = 0.1

    def __init__(
        self,
        endpoint: str,
        uid: str = None,
        client_id: str = None,
        client_secret: str = None,
        timeout=None,
        allowed_pickle_classes=(),
    ):
        self.allowed_pickle_classes = tuple(allowed_pickle_classes)
        if not all(isinstance(cls, type)
                   for cls in self.allowed_pickle_classes):
            raise TypeError('allowed_pickle_classes must contain classes')
        if client_id is None and client_secret is None:
            client_id = environ.get('STOPOVER_CLIENT_ID')
            client_secret = environ.get('STOPOVER_CLIENT_SECRET')
        if (client_id is None) != (client_secret is None):
            raise ValueError('Both Stopover client credentials are required')
        if timeout is None:
            timeout = (
                environ.get("STOPOVER_CONNECT_TIMEOUT", 3),
                environ.get("STOPOVER_READ_TIMEOUT", 30),
            )
        elif isinstance(timeout, (int, float)):
            timeout = (timeout, timeout)
        if not isinstance(timeout, (tuple, list)) or len(timeout) != 2:
            raise ValueError(
                "timeout must be a number or a (connect, read) pair"
            )
        if any(isinstance(value, bool) for value in timeout):
            raise ValueError(
                "Stopover timeouts must be positive finite seconds"
            )
        try:
            self.timeout = tuple(float(value) for value in timeout)
        except (TypeError, ValueError):
            raise ValueError(
                "Stopover timeouts must be positive finite seconds"
            ) from None
        if any(
            not math.isfinite(value) or value <= 0 for value in self.timeout
        ):
            raise ValueError(
                "Stopover timeouts must be positive finite seconds"
            )
        self.endpoint = endpoint
        if uid is not None:
            self._uid = uid
        else:
            self._uid = get_uid()

        self.temporary_offsets = {}
        self.session = requests.Session()

        if client_id is not None and client_secret is not None:
            self.session.headers.update(
                {
                    "Authorization": (
                        "Basic "
                        + base64.b64encode(
                            f"{client_id}:{client_secret}".encode("ascii")
                        ).decode("ascii")
                    )
                }
            )

    @property
    def uid(self):
        return self._uid

    def health(self):
        """Authenticated, bounded readiness without consuming/committing input."""
        with self.session.post(self.endpoint, json={'method': 'health', 'params': {}},
                               timeout=(0.5, 0.5), allow_redirects=False) as response:
            return response.status_code == 200 and response.json().get('status') == STATUS.OK

    def put(
        self,
        value,
        stream: str,
        key: str = None,
    ) -> dict:
        if not isinstance(value, (str, int, float, bytes, bool)):
            value = pickle.dumps(value, protocol=5)

        response = self._put_call(value, stream, key=key)
        if response.status not in [STATUS.OK, "OK", "ok"]:
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
        if response.status not in [STATUS.OK, "OK", "ok"]:
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
            if response.status not in [STATUS.OK, "OK", "ok"]:
                time.sleep(Stopover.LISTEN_INTERVAL)
                continue
            yield response

    def commit(
        self,
        message: Response,
        receiver_group: str,
    ):
        response = self._commit_call(message, receiver_group)
        if response.status not in [STATUS.OK, "OK", "ok"]:
            raise errors.CommitError(response.status)
        return response

    def knock(
        self,
        receiver_group: str,
        receiver: str = None,
    ):
        response = self._knock_call(receiver_group, receiver=receiver)
        if response.status not in [STATUS.OK, "OK", "ok"]:
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
            skip_pruned=progress_without_commit,
        )

        if progress_without_commit:
            if response.status in [STATUS.OK, "OK", "ok"] and (
                response.partition not in response.assigned_partitions
                or (partition is not None and response.partition != partition)
            ):
                raise errors.GetError(
                    "Server does not support partition-aware progress"
                )
            if response.status in [
                STATUS.OK,
                "OK",
                "ok",
                STATUS.END_OF_STREAM,
                STATUS.ALL_PARTITIONS_ASSIGNED,
            ]:
                self._update_temporary_offsets(
                    response, advance=not auto_commit
                )

        if response.status in [STATUS.OK, "OK", "ok"]:
            if isinstance(response.value, bytes):
                try:
                    response.value = MessageUnpickler(
                        response.value, self.allowed_pickle_classes).load()
                except Exception:
                    pass

            if auto_commit:
                self.commit(response, receiver_group)
                if progress_without_commit:
                    self._update_temporary_offsets(response)

        return response

    @raise_connection_error
    def _put_call(
        self,
        value: bytes,
        stream: str,
        key: str = None,
    ) -> dict:
        data = {
            "method": "put_message",
            "params": {
                "value": value,
                "stream": stream,
            },
        }
        if key is not None:
            data["params"]["key"] = key
        response = self.session.post(
            self.endpoint, data=compress(pack(data)), timeout=self.timeout
        )

        if response.status_code != 200:
            raise errors.ServerConnectionError(f"HTTP {response.status_code}")

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
            receiver
        ]

        assigned_partitions = list(temporary_offsets.keys())
        if assigned_partitions:
            partition = assigned_partitions[
                random.randint(
                    0,
                    len(assigned_partitions) - 1,
                )
            ]

            index = (
                None
                if partition not in temporary_offsets
                else temporary_offsets[partition]
            )

        return partition, index

    def _update_temporary_offsets(self, response, advance=True):
        temporary_offsets = self.temporary_offsets[response.stream][
            response.receiver_group
        ][response.receiver]
        if response.status in [STATUS.OK, "OK", "ok"]:
            temporary_offsets[response.partition] = response.index + int(
                advance
            )

        for assigned_partition in list(temporary_offsets):
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
        skip_pruned: bool = False,
    ) -> Response:
        data = {
            "method": "get_message",
            "params": {
                "stream": stream,
                "receiver_group": receiver_group,
                "receiver": receiver,
                "partition": partition,
                "index": index,
            },
        }
        if skip_pruned:
            data["params"]["skip_pruned"] = True
        response = self.session.post(
            self.endpoint, data=compress(pack(data)), timeout=self.timeout
        )

        if response.status_code != 200:
            raise errors.ServerConnectionError(f"HTTP {response.status_code}")

        return Response(**unpack(decompress(response.content)))

    def _commit_call(
        self,
        message: Response,
        receiver_group: str,
    ):
        data = {
            "method": "commit_message",
            "params": {
                "stream": message.stream,
                "partition": message.partition,
                "index": message.index,
                "receiver_group": receiver_group,
            },
        }

        response = self.session.post(
            self.endpoint, data=compress(pack(data)), timeout=self.timeout
        )

        if response.status_code != 200:
            raise errors.ServerConnectionError(f"HTTP {response.status_code}")

        return Response(**unpack(decompress(response.content)))

    @raise_connection_error
    def _knock_call(
        self,
        receiver_group: str,
        receiver: str = None,
    ):
        receiver = receiver if receiver else self.uid
        data = {
            "method": "knock",
            "params": {"receiver_group": receiver_group, "receiver": receiver},
        }

        response = self.session.post(
            self.endpoint, data=compress(pack(data)), timeout=self.timeout
        )

        if response.status_code != 200:
            raise errors.ServerConnectionError(f"HTTP {response.status_code}")

        return Response(**unpack(decompress(response.content)))
