"""Synthetic regressions for offline probe and container source boundaries.

Uses private temporary source trees and numeric loopback HTTP servers only.
It does not run Docker, a real account or a production protocol worker.
"""
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.error

from offline_api import request_json


ROOT = Path(__file__).resolve().parent.parent


def load_tool(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), ROOT / 'tools' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


go_checks = load_tool('container-go-checks')
runtime_checks = load_tool('container-offline-checks')


class ProbeTests(unittest.TestCase):
    def test_redirects_never_contact_another_origin(self):
        received = []
        source_received = []
        redirect = {'code': 302, 'destination': ''}

        class Target(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(dict(self.headers))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')

            def log_message(self, *args):
                pass

        class Source(Target):
            def do_GET(self):
                source_received.append(self.headers.get('Authorization'))
                self.send_response(redirect['code'])
                self.send_header('Location', redirect['destination'])
                self.end_headers()

        target = ThreadingHTTPServer(('127.0.0.1', 0), Target)
        source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
        workers = [threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
                   for server in [target, source]]
        for worker in workers:
            worker.start()
        try:
            base = 'http://127.0.0.1:' + str(source.server_port)
            for destination in ['http://127.0.0.1:' + str(target.server_port),
                                'http://192.0.2.1:1/never-contact']:
                for status in [301, 302, 303, 307, 308]:
                    with self.subTest(destination=destination, status=status):
                        redirect.update(code=status, destination=destination)
                        with self.assertRaises(urllib.error.HTTPError) as error:
                            request_json(base, 'synthetic-only-token')
                        self.assertEqual(error.exception.code, status)
                        error.exception.close()
            self.assertEqual(source_received, ['Bearer synthetic-only-token'] * 10)
            self.assertEqual(received, [], 'redirect target received a request/token')
        finally:
            for server in [target, source]:
                server.shutdown()
                server.server_close()
            for worker in workers:
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive())

    def test_initial_external_or_credential_urls_are_rejected(self):
        for url in ['http://192.0.2.1/', 'http://localhost/', 'https://127.0.0.1/',
                    'http://user:password@127.0.0.1/', 'http://127.0.0.1/#fragment']:
            with self.subTest(url=url), self.assertRaises(ValueError):
                request_json(url, 'synthetic-only-token')

    def test_embedded_catalog_and_crypto_key_changes_reject_stale_builder(self):
        with tempfile.TemporaryDirectory(prefix='container-source-regression-') as raw:
            source = Path(raw)
            inputs = {'go.mod': b'module fixture\n',
                      'internal/catalog/actions.json': b'[{"name":"original"}]',
                      'internal/cdn/legacy-public-key.der': b'original-synthetic-DER',
                      'internal/assets/manage.html': b'<p>fixture</p>'}
            for name, body in inputs.items():
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
            builder = go_checks.manifest(source)
            self.assertEqual(set(builder), set(inputs))
            go_checks.require_matching_source(source, builder)
            for name in ['internal/catalog/actions.json', 'internal/cdn/legacy-public-key.der']:
                with self.subTest(input=name):
                    path = source / name
                    path.write_bytes(inputs[name] + b'changed')
                    with self.assertRaisesRegex(ValueError, name):
                        go_checks.require_matching_source(source, builder)
                    path.write_bytes(inputs[name])
            go_checks.require_matching_source(source, builder)

    def test_host_metadata_disables_toolchain_download_and_has_deadline(self):
        with patch.object(runtime_checks.subprocess, 'check_output', return_value='fixture') as run:
            self.assertEqual(runtime_checks.binary_build_info(Path('/synthetic/binary')), 'fixture')
        args, kwargs = run.call_args
        self.assertEqual(args[0], ['go', 'version', '-m', '/synthetic/binary'])
        self.assertEqual(kwargs['timeout'], 30)
        for key, value in {'GOTOOLCHAIN': 'local', 'GOPROXY': 'off', 'GOSUMDB': 'off',
                           'GOENV': 'off', 'GOTELEMETRY': 'off'}.items():
            self.assertEqual(kwargs['env'][key], value)


if __name__ == '__main__':
    unittest.main()
