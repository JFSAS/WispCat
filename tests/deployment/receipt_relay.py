"""Transparent owned-loopback WS relay, withholding one accepted send reply."""
import asyncio
import hashlib
import ipaddress
import json
import threading
import urllib.parse

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from receipt_reply import PREFIX


def target_url(url):
    value = urllib.parse.urlsplit(url)
    assert value.scheme == 'ws' and value.hostname and ipaddress.ip_address(value.hostname).is_loopback
    assert value.port and not value.username and not value.password and not value.query and not value.fragment
    assert value.path in ('/ws', '/onebot/v11/ws')
    return value


class ReceiptRelay:
    def __init__(self, target):
        self.target = target
        self.path = target_url(target).path
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.held = threading.Event()
        self.reply = None
        self.forwarded_confirmed_replies = 0
        self.requests = []
        self.pairs = {}
        self.failures = []
        self.thread = threading.Thread(target=self._thread)
        self.thread.start()
        if not self.ready.wait(5):
            self.close()
            raise TimeoutError('receipt relay did not start')
        assert not self.failures, self.failures

    def _thread(self):
        try:
            asyncio.run(self._run())
        except BaseException as error:
            with self.lock:
                self.failures.append(type(error).__name__)
        finally:
            self.ready.set()

    async def _run(self):
        self.loop = asyncio.get_running_loop()
        self.stopping = asyncio.Event()
        async with serve(self._handle, '127.0.0.1', 0, max_size=24 << 20,
                         max_queue=4, ping_interval=None, close_timeout=1) as server:
            self.url = f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}' + self.path
            self.ready.set()
            await self.stopping.wait()

    async def _handle(self, runtime):
        peer = None
        tasks = []
        identity = None
        try:
            assert runtime.request.path == self.path
            headers = {name: runtime.request.headers[name] for name in ['X-Self-ID', 'X-Client-Role', 'Authorization']}
            identity = headers['X-Self-ID']
            assert identity.isdigit() and headers['X-Client-Role'] == 'Universal'
            peer = await connect(self.target, additional_headers=headers, proxy=None,
                                 max_size=24 << 20, max_queue=4, compression=None,
                                 open_timeout=3, close_timeout=1, ping_interval=None)
            with self.lock:
                self.pairs[identity] = self.pairs.get(identity, 0) + 1

            async def inbound():
                async for raw in peer:
                    request = json.loads(raw)
                    key = request.get('params', {}).get('wechat_request_id', '')
                    if isinstance(key, str) and key.startswith(PREFIX):
                        with self.lock:
                            self.requests.append({'self_id': identity, 'action': request['action'], 'request_id': key, 'echo': request.get('echo')})
                    await runtime.send(raw)

            async def outbound():
                async for raw in runtime:
                    response = json.loads(raw)
                    data = response.get('data')
                    key = data.get('wechat_request_id', '') if isinstance(data, dict) else ''
                    targeted = isinstance(key, str) and key.startswith(PREFIX)
                    accepted = targeted and response.get('status') == 'ok' and response.get('retcode') == 0 and data.get('message_id') and data.get('wechat_delivery_id')
                    hold = False
                    with self.lock:
                        if accepted and self.reply is None:
                            encoded = raw.encode() if isinstance(raw, str) else raw
                            self.reply = {'self_id': identity, 'response': response,
                                          'raw_sha256': hashlib.sha256(encoded).hexdigest(), 'forwarded': False}
                            self.held.set()
                            hold = True
                    if hold:
                        # Drop only this reply. Heartbeats, other results and
                        # events must keep flowing on the same connection.
                        continue
                    await peer.send(raw)
                    if accepted:
                        with self.lock:
                            self.forwarded_confirmed_replies += 1

            tasks = [asyncio.create_task(inbound()), asyncio.create_task(outbound()),
                     asyncio.create_task(runtime.wait_closed())]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        except (ConnectionClosed, ConnectionRefusedError, TimeoutError):
            pass
        except Exception as error:
            with self.lock:
                self.failures.append(type(error).__name__)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if peer is not None:
                with self.lock:
                    self.pairs[identity] -= 1
                    if not self.pairs[identity]:
                        del self.pairs[identity]
                await peer.close()
            await runtime.close()

    def snapshot(self):
        with self.lock:
            return {'held_reply': self.reply, 'forwarded_confirmed_replies': self.forwarded_confirmed_replies,
                    'requests': list(self.requests), 'ready_ids': sorted(key for key, count in self.pairs.items() if count == 1),
                    'failures': list(self.failures)}

    def close(self):
        if hasattr(self, 'loop') and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.stopping.set)
        self.thread.join(timeout=5)
        assert not self.thread.is_alive(), 'receipt relay did not join'
        assert not self.failures, self.failures
