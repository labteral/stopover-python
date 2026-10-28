"""Protocol compatibility: a legacy server must not silently skip intake."""

import unittest
from unittest.mock import Mock
from stopover import Stopover, Response, errors


class ProgressTests(unittest.TestCase):
    def test_legacy_reads_work_but_unsafe_progress_is_rejected_before_ack(self):
        client = Stopover('http://127.0.0.1:9', uid='fixture')
        self.addCleanup(client.session.close)
        client._get_call = Mock(return_value=Response(status=20, stream='intake', receiver_group='group',
            receiver='fixture', partition=0, index=0, value='kept', assigned_partitions=[]))
        client.commit = Mock()
        self.assertEqual(client.get('intake', 'group').value, 'kept')
        with self.assertRaisesRegex(errors.GetError, 'partition-aware'):
            client.get('intake', 'group', progress_without_commit=True, auto_commit=True)
        client.commit.assert_not_called()
        self.assertEqual(client.temporary_offsets['intake']['group']['fixture'], {})

    def test_listen_uses_the_same_partition_validation(self):
        client = Stopover('http://127.0.0.1:9', uid='fixture')
        self.addCleanup(client.session.close)
        client._get_call = Mock(return_value=Response(status=20, stream='intake', receiver_group='group',
            receiver='fixture', partition=1, index=4, value='kept', assigned_partitions=[0, 1]))
        client._get_next_partition_and_index = Mock(return_value=(0, 4))
        with self.assertRaises(errors.GetError):
            next(client.listen('intake', 'group', progress_without_commit=True))


if __name__ == '__main__':
    unittest.main()
