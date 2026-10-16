"""Safety and ownership checks for the deliberate-crash fixture."""
import http.client
import os
import unittest
from unittest.mock import patch

from crash_reply import source_for
from crash_source import StalledSource, cancellation_window


class CrashProbeTests(unittest.TestCase):
    def test_near_timeout_crash_cannot_count_as_cancellation_evidence(self):
        timing = cancellation_window(100, 100.5, 101)
        self.assertEqual(timing['minimum_natural_timeout_margin_millis'], 4000)
        for entered, killed in [(100.5, 104), (108, 108.5), (100.5, 110), (99, 101), (102, 101)]:
            with self.subTest(entered=entered, killed=killed), self.assertRaises(AssertionError):
                cancellation_window(100, entered, killed)

    def test_command_requires_explicit_numeric_loopback_source(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(source_for('/crash-transfer'))
        with patch.dict(os.environ, {'DEPLOYMENT_CRASH_SOURCE': 'http://127.0.0.1:12345/crash-source'}):
            self.assertIsNone(source_for('/private-ping'))
            self.assertEqual(source_for('/crash-transfer'), 'http://127.0.0.1:12345/crash-source')
        for value in ['http://192.0.2.1:12345/crash-source', 'http://example.invalid/crash-source',
                      'https://127.0.0.1:12345/crash-source', 'http://user@127.0.0.1:12345/crash-source',
                      'http://127.0.0.1:12345/other', 'http://127.0.0.1:12345/crash-source?x=1']:
            with self.subTest(value=value), patch.dict(os.environ, {'DEPLOYMENT_CRASH_SOURCE': value}):
                with self.assertRaises((AssertionError, ValueError)):
                    source_for('/crash-transfer')

    def test_remote_body_close_is_observed_and_threads_join(self):
        source = StalledSource()
        client = http.client.HTTPConnection('127.0.0.1', source.server.server_port, timeout=2)
        try:
            client.request('GET', '/crash-source', headers={'User-Agent': 'synthetic-source-client'})
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(1), b'x')
            self.assertTrue(source.entered.wait(1))
            self.assertFalse(source.closed.is_set())
            response.close()
            client.close()
            self.assertTrue(source.closed.wait(1))
        finally:
            client.close()
            source.close()
        self.assertFalse(source.thread.is_alive())

    def test_local_cleanup_does_not_masquerade_as_runtime_cancellation(self):
        source = StalledSource()
        client = http.client.HTTPConnection('127.0.0.1', source.server.server_port, timeout=2)
        try:
            client.request('GET', '/crash-source')
            response = client.getresponse()
            self.assertEqual(response.read(1), b'x')
            self.assertTrue(source.entered.wait(1))
            source.close()
            self.assertFalse(source.closed.is_set())
            self.assertIsNone(source.closed_at)
        finally:
            client.close()
            source.close()
        self.assertFalse(source.thread.is_alive())


if __name__ == '__main__':
    unittest.main()
