"""Actual loopback forwarding/hold/teardown and consumer-record safety."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from receipt_reply import PREFIX, enabled, parameters, request_for
from receipt_relay import ReceiptRelay, target_url


class ReceiptRecordTests(unittest.TestCase):
    def test_private_record_restores_same_semantics_and_rejects_other_owner(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            'DEPLOYMENT_RECEIPT_LOSS': '1', 'DEPLOYMENT_MEDIA_PROFILE': 'small',
            'DEPLOYMENT_RECEIPT_RECORD': directory+'/request.json'}):
            first, before = request_for('/receipt-send', 1001, 1002, 'Synthetic Friend synthetic-a')
            again, after = request_for('/receipt-retry', 1001, 1002, 'Synthetic Friend synthetic-a')
            self.assertEqual(first, again)
            self.assertEqual(before, after)
            self.assertEqual(Path(directory+'/request.json').stat().st_mode & 0o777, 0o600)
            with self.assertRaises(AssertionError):
                request_for('/receipt-retry', 1003, 1002, 'Synthetic Friend synthetic-a')
            with self.assertRaises(AssertionError):
                parameters({**before, 'sha256': '00'*32})

    def test_disabled_command_and_symlink_record_are_not_accepted(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(enabled('/receipt-send'))
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            'DEPLOYMENT_RECEIPT_LOSS': '1', 'DEPLOYMENT_MEDIA_PROFILE': 'small',
            'DEPLOYMENT_RECEIPT_RECORD': directory+'/request.json'}):
            Path(directory+'/other').write_text('{}')
            Path(directory+'/request.json').symlink_to('other')
            with self.assertRaises(OSError):
                request_for('/receipt-retry', 1001, 1002, 'Synthetic Friend synthetic-a')

    def test_relay_target_must_be_owned_numeric_loopback_ws(self):
        for value in ['ws://192.0.2.1:12345/ws', 'ws://example.invalid/ws',
                      'wss://127.0.0.1:12345/ws', 'ws://user@127.0.0.1:12345/ws',
                      'ws://127.0.0.1:12345/other', 'ws://127.0.0.1:12345/ws?x=1']:
            with self.subTest(value=value), self.assertRaises((AssertionError, ValueError)):
                target_url(value)


class ReceiptRelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.peers = asyncio.Queue()
        async def accept(peer):
            await self.peers.put(peer)
            await peer.wait_closed()
        self.server = await serve(accept, '127.0.0.1', 0, ping_interval=None, close_timeout=1)
        self.relay = ReceiptRelay(f'ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/ws')
        self.clients = []

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()
        await asyncio.to_thread(self.relay.close)
        self.server.close()
        await self.server.wait_closed()

    async def pair(self):
        runtime = await connect(self.relay.url, proxy=None, additional_headers={
            'X-Self-ID': '1001', 'X-Client-Role': 'Universal', 'Authorization': 'Bearer synthetic'})
        self.clients.append(runtime)
        peer = await asyncio.wait_for(self.peers.get(), 2)
        self.assertEqual(peer.request.headers['X-Self-ID'], '1001')
        self.assertEqual(peer.request.headers['Authorization'], 'Bearer synthetic')
        return runtime, peer

    def response(self, echo):
        return json.dumps({'status': 'ok', 'retcode': 0, 'echo': echo, 'data': {
            'wechat_request_id': PREFIX+'stable', 'message_id': 1200, 'wechat_delivery_id': 'synthetic-delivery'}}, ensure_ascii=False)

    async def test_hold_never_replays_and_new_connection_preserves_exact_frames(self):
        runtime, peer = await self.pair()
        request = json.dumps({'action': 'send_private_msg', 'params': {'wechat_request_id': PREFIX+'stable'},
                              'echo': {'id': 9007199254740993, 'text': '原样'}}, ensure_ascii=False, indent=2)
        await peer.send(request)
        self.assertEqual(await runtime.recv(), request)
        await runtime.send(self.response({'id': 9007199254740993, 'text': '原样'}))
        self.assertTrue(await asyncio.to_thread(self.relay.held.wait, 2))
        pending = asyncio.create_task(peer.recv())
        await asyncio.sleep(.05)
        self.assertFalse(pending.done(), 'withheld response reached the consumer')
        await peer.close()
        await asyncio.gather(pending, return_exceptions=True)
        await asyncio.wait_for(runtime.wait_closed(), 2)
        runtime, peer = await self.pair()
        retry = request.replace('9007199254740993', '9007199254740994')
        await peer.send(retry)
        self.assertEqual(await runtime.recv(), retry)
        response = self.response({'id': 9007199254740994, 'text': '原样'})
        await runtime.send(response)
        self.assertEqual(await asyncio.wait_for(peer.recv(), 2), response)
        snapshot = self.relay.snapshot()
        self.assertFalse(snapshot['held_reply']['forwarded'])
        self.assertEqual(snapshot['forwarded_confirmed_replies'], 1)
        self.assertEqual(len(snapshot['requests']), 2)

    async def test_failure_and_meta_frames_forward_without_becoming_success(self):
        runtime, peer = await self.pair()
        for raw in ['{"post_type":"meta_event","self_id":1001}',
                    json.dumps({'status': 'failed', 'retcode': 100, 'data': {'wechat_request_id': PREFIX+'stable'}})]:
            await runtime.send(raw)
            self.assertEqual(await asyncio.wait_for(peer.recv(), 2), raw)
        self.assertFalse(self.relay.held.is_set())

    async def test_shutdown_joins_even_when_response_is_held(self):
        runtime, peer = await self.pair()
        await runtime.send(self.response('held-during-close'))
        self.assertTrue(await asyncio.to_thread(self.relay.held.wait, 2))
        await asyncio.to_thread(self.relay.close)
        await asyncio.wait_for(runtime.wait_closed(), 2)
        await asyncio.wait_for(peer.wait_closed(), 2)
        self.assertFalse(self.relay.thread.is_alive())

    async def test_lost_reply_does_not_block_later_frames_on_same_connection(self):
        runtime, peer = await self.pair()
        await runtime.send(self.response('lost'))
        self.assertTrue(await asyncio.to_thread(self.relay.held.wait, 2))
        for raw in ['{"post_type":"meta_event","meta_event_type":"heartbeat"}',
                    '{"status":"ok","retcode":0,"echo":"other","data":{"good":true}}']:
            await runtime.send(raw)
            self.assertEqual(await asyncio.wait_for(peer.recv(), 2), raw)
        self.assertFalse(self.relay.snapshot()['held_reply']['forwarded'])


if __name__ == '__main__':
    unittest.main()
