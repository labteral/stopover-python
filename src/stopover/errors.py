#!/usr/bin/env python
# -*- coding: utf-8 -*-


class PutError(Exception):
    pass


class CommitError(Exception):
    pass


class GetError(Exception):
    pass


class ServerConnectionError(Exception):
    pass


class KnockError(Exception):
    pass
