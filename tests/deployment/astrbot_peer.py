"""Actual AstrBot platform adapter, conversion and send, with a local consumer.

No full application lifecycle, provider, model, dashboard or plugin is started.
SIGTERM explicitly joins the actual adapter/server before a zero exit.
Dependency imports and AstrBot's default private files stay in the temporary root.
"""
import asyncio
import base64
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import sys

SOURCE = Path(os.environ['DEPLOYMENT_ASTRBOT_SOURCE'])
REVISION = '9441402872f65fffc35685b0ead5bd3337033cb2'
assert os.environ['DEPLOYMENT_ASTRBOT_REVISION'] == REVISION
sys.path.insert(0, str(SOURCE))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'framework'))
os.environ['ASTRBOT_DISABLE_METRICS'] = '1'

from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain, Image, File
from astrbot.core.config import VERSION
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import AiocqhttpAdapter

report = Path(os.environ['FRAMEWORK_REPORT'])
from stream_probe import probe
from media_probe import local_media_url,payload,label_from_sender


async def main():
    queue = asyncio.Queue()
    adapter = AiocqhttpAdapter({'id': 'synthetic-platform', 'ws_reverse_host': '127.0.0.1',
        'ws_reverse_port': int(os.environ['FRAMEWORK_PORT']),
        'ws_reverse_token': 'synthetic-framework-token'}, {}, queue)
    handled = []
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stopping.set)

    async def consume():
        while True:
            event = await queue.get()
            text = event.message_str.strip()
            media_parts=[part for part in event.message_obj.message if isinstance(part,(Image,File))]
            if not media_parts and text not in ('/private-ping', '/group-ping'):
                continue
            message = event.message_obj
            group = bool(event.get_group_id())
            mentions = [part for part in message.message if isinstance(part, At)]
            to_me = not group or any(str(part.qq) == message.self_id for part in mentions)
            if group:
                assert len(mentions) == 1 and mentions[0].name, message.message
                assert to_me, message.message
            login = await adapter.bot.get_login_info(self_id=message.self_id)
            status = await adapter.bot.get_status(self_id=message.self_id)
            assert int(login['user_id']) == int(message.self_id)
            assert status['online'] and status['good']
            stream_verified = await probe(lambda action, **params: getattr(adapter.bot, action)(**params), message.self_id, message.message_id)
            extra={}
            if media_parts:
                assert len(media_parts)==1
                part=media_parts[0]
                local_media_url(part.url)
                if isinstance(part,Image):
                    kind='image';content=base64.b64decode(await part.convert_to_base64(),validate=True)
                    chain=[Image.fromBytes(content)]
                else:
                    kind='file';content=Path(await part.get_file()).read_bytes()
                    chain=[File(name='reply.bin',url=part.url)]
                assert content==payload(kind,label_from_sender(message.sender.nickname))
                original=next(p for p in message.raw_message['message'] if p['type']==kind)
                extra={'media':kind,'media_sha256':hashlib.sha256(content).hexdigest(),'received_file_id':str(original['data']['file'])}
            else:
                chain = ([At(qq=event.get_sender_id())] if group else []) + [Plain(text='pong:' + text)]
            await event.send(MessageChain(chain))
            handled.append({'kind': 'group' if group else 'private',
                'user_id': int(event.get_sender_id()), 'self_id': int(message.self_id),
                'to_me': to_me, 'member_lookup_verified': group,
                'stream_upload_verified': stream_verified,
                'message_id': int(message.message_id),'received_message_id':int(message.message_id),**extra})
            report.write_text(json.dumps({'framework': 'astrbot',
                'scope': 'actual platform adapter, message conversion and send; local consumer replaces app pipeline',
                'source_revision': REVISION, 'versions': {'AstrBot': VERSION,
                    **{name: importlib.metadata.version(name) for name in ['aiocqhttp', 'quart', 'hypercorn']}},
                'metrics_disabled': True, 'handled': handled}, indent=2))

    consumer = asyncio.create_task(consume())
    server = asyncio.create_task(adapter.run())
    stop_wait = asyncio.create_task(stopping.wait())
    try:
        finished, _ = await asyncio.wait([consumer, server, stop_wait], return_when=asyncio.FIRST_COMPLETED)
        if consumer in finished:
            await consumer
        if server in finished and not stopping.is_set():
            await server
            raise RuntimeError('adapter exited before requested shutdown')
    finally:
        await adapter.terminate()
        await asyncio.wait_for(server, timeout=10)
        consumer.cancel()
        stop_wait.cancel()
        await asyncio.gather(consumer, stop_wait, return_exceptions=True)
        if report.exists():
            value = json.loads(report.read_text())
            value.update(adapter_shutdown=True, server_joined=True)
            report.write_text(json.dumps(value, indent=2) + '\n')


if __name__ == '__main__':
    asyncio.run(main())
