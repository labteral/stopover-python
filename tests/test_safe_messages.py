import base64
from collections import OrderedDict
import copyreg
import os
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import requests
from stopover import Stopover, Response, errors


class Data:
    def __init__(self, value):
        self.value = value


class SafeMessageTests(unittest.TestCase):
    def client(self, value, **kwargs):
        client = Stopover('http://fixture.invalid', **kwargs)
        self.addCleanup(client.session.close)
        client._get_call = Mock(return_value=Response(status=20, value=value))
        return client

    def test_legacy_primitives_and_explicit_data_classes_roundtrip(self):
        value = OrderedDict([('items', [1, 'text', b'bytes']), ('tuple', (1, 2))])
        self.assertEqual(self.client(pickle.dumps(value, protocol=5)).get('s', 'g').value, value)
        payload = pickle.dumps(Data('kept'), protocol=5)
        self.assertEqual(self.client(payload).get('s', 'g').value, payload)
        decoded = self.client(payload, allowed_pickle_classes=(Data,)).get('s', 'g').value
        self.assertIsInstance(decoded, Data)
        self.assertEqual(decoded.value, 'kept')
        with self.assertRaises(TypeError):
            self.client(payload, allowed_pickle_classes=(eval,))

    def test_pickle_globals_and_warm_extension_cache_cannot_execute(self):
        with TemporaryDirectory(prefix='stopover-pickle-') as directory:
            marker = Path(directory) / 'executed'
            expression = f"__import__('pathlib').Path({str(marker)!r}).write_text('unsafe')"

            class Payload:
                def __reduce__(self):
                    return eval, (expression,)

            ordinary = pickle.dumps(Payload(), protocol=5)
            extension = b'\x80\x02\x82\x2a' + pickle.dumps((expression,), protocol=2)[2:-1] + b'R.'
            with patch.dict(copyreg._extension_cache, {42: eval}):
                for payload in (ordinary, extension):
                    self.assertEqual(self.client(payload).get('s', 'g').value, payload)
                    self.assertFalse(marker.exists())

    def test_credentials_can_come_from_environment_and_partial_pairs_fail(self):
        with patch.dict(os.environ, {'STOPOVER_CLIENT_ID': 'fixture', 'STOPOVER_CLIENT_SECRET': 'fixture-secret'}):
            client = self.client(b'data')
            expected = 'Basic ' + base64.b64encode(b'fixture:fixture-secret').decode()
            self.assertEqual(client.session.headers['Authorization'], expected)
            with self.assertRaises(ValueError):
                self.client(b'data', client_id='explicit-only')

    def test_connection_errors_do_not_expose_endpoint_credentials(self):
        client = Stopover('http://user:secret@fixture.invalid/path?key=private')
        self.addCleanup(client.session.close)
        with patch.object(client.session, 'post', side_effect=requests.ConnectTimeout()):
            with self.assertRaises(errors.ServerConnectionError) as caught:
                client.get('s', 'g')
        self.assertIn('fixture.invalid/path', str(caught.exception))
        self.assertNotIn('secret', str(caught.exception))
        self.assertNotIn('private', str(caught.exception))
