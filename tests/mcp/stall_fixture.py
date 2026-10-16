"""Owned loopback HTTP fixture that observes a cancelled download socket."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import socket
import threading


class StalledOneBot:
    def __init__(self):
        self.started=threading.Event();self.cancelled=threading.Event();self.release=threading.Event()
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                size=int(self.headers.get('Content-Length','0'));assert 0<=size<=1<<20
                self.rfile.read(size)
                if self.path=='/get_login_info':
                    body=json.dumps({'status':'ok','retcode':0,'data':{'user_id':123}}).encode()
                    self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)))
                    self.end_headers();self.wfile.write(body);return
                assert self.path=='/download_file_stream'
                self.send_response(200);self.send_header('Content-Type','application/x-ndjson');self.end_headers();self.wfile.flush()
                owner.started.set();self.connection.settimeout(.1)
                while not owner.release.is_set():
                    try:
                        if self.connection.recv(1,socket.MSG_PEEK)==b'':
                            owner.cancelled.set();return
                    except socket.timeout:pass
                    except ConnectionResetError:
                        owner.cancelled.set();return
            def log_message(self,*_):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever)
        self.url=f'http://127.0.0.1:{self.server.server_port}'

    def __enter__(self):
        self.thread.start();return self

    def __exit__(self,*_):
        self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=5)
        assert not self.thread.is_alive()
