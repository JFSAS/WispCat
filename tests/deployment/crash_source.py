"""Owned loopback source that exposes cancellation of a Go action body read."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
import time


def cancellation_window(command_at, entered_at, kill_at):
    # The request cannot start before its input command is injected. Bounding
    # that whole interval avoids mistaking fetchMedia's existing10s timeout
    # for crash-triggered cancellation within the following2s observation.
    assert command_at <= entered_at <= kill_at
    assert kill_at-command_at < 4, 'crash too late to distinguish cancellation from the existing source timeout'
    return {'command_to_kill_millis': int((kill_at-command_at)*1000),
            'source_entered_to_kill_millis': int((kill_at-entered_at)*1000),
            'existing_source_timeout_millis': 10000, 'cancellation_observation_millis': 2000,
            'minimum_natural_timeout_margin_millis': 4000}


class StalledSource:
    def __init__(self):
        self.entered = threading.Event()
        self.closed = threading.Event()
        self.requests = []
        self.connections = []
        self.lock = threading.Lock()
        self.declared_bytes = 8388645
        self.closed_at = None
        self.entered_at = None
        self.closing = threading.Event()
        owner = self

        class Server(ThreadingHTTPServer):
            daemon_threads = False

            def get_request(self):
                connection, address = super().get_request()
                with owner.lock:
                    owner.connections.append(connection)
                return connection, address

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != '/crash-source':
                    self.send_error(404)
                    return
                with owner.lock:
                    owner.requests.append({'path': self.path, 'user_agent': self.headers.get('User-Agent', '')})
                self.connection.settimeout(12)
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(owner.declared_bytes))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(b'x')
                self.wfile.flush()
                owner.entered_at = time.monotonic()
                owner.entered.set()
                try:
                    # A GET has no request body. EOF here means the runtime
                    # closed its incomplete response rather than reading EOF.
                    if self.connection.recv(1) == b'' and not owner.closing.is_set():
                        owner.closed_at = time.monotonic()
                        owner.closed.set()
                except (ConnectionResetError, BrokenPipeError):
                    if not owner.closing.is_set():
                        owner.closed_at = time.monotonic()
                        owner.closed.set()
                except TimeoutError:
                    pass
                except OSError:
                    if not owner.closing.is_set():
                        raise

            def log_message(self, *_):
                pass

        self.server = Server(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_port}/crash-source'
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def close(self):
        self.closing.set()
        self.server.shutdown()
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        self.server.server_close()
        self.thread.join(timeout=2)
        assert not self.thread.is_alive(), 'stalled source did not join'
