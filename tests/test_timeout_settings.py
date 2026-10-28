"""Timeout settings are finite, positive and validated before session creation."""

import os
import unittest
from unittest.mock import patch
from stopover import Stopover


class TimeoutSettingsTests(unittest.TestCase):
    def test_settings_are_validated_and_defaults_apply_to_all_clients(self):
        for value in (0, -1, True, float('nan'), (1, 0), (None, 1), (1,), 'bad'):
            with self.subTest(value=value), patch('stopover.stopover.requests.Session') as session:
                with self.assertRaises(ValueError):
                    Stopover('http://127.0.0.1:9', timeout=value)
                session.assert_not_called()
        for key in ('STOPOVER_CONNECT_TIMEOUT', 'STOPOVER_READ_TIMEOUT'):
            with patch.dict(os.environ, {key: 'inf'}), self.assertRaises(ValueError):
                Stopover('http://127.0.0.1:9')
        with patch.dict(os.environ, {}, clear=True):
            default = Stopover('http://127.0.0.1:9')
        self.addCleanup(default.session.close)
        self.assertEqual(default.timeout, (3, 30))
        with patch.dict(os.environ, {'STOPOVER_CONNECT_TIMEOUT': '.2', 'STOPOVER_READ_TIMEOUT': '.05'}):
            client = Stopover('http://127.0.0.1:9')
        self.addCleanup(client.session.close)
        self.assertEqual(client.timeout, (.2, .05))
        scalar = Stopover('http://127.0.0.1:9', timeout=.1)
        self.addCleanup(scalar.session.close)
        self.assertEqual(scalar.timeout, (.1, .1))


if __name__ == '__main__':
    unittest.main()
