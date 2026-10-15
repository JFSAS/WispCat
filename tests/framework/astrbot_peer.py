"""Actual AstrBot platform adapter, conversion and send, with a local consumer.

No application lifecycle, provider, model, dashboard or plugin is started.
Dependency imports and AstrBot's default private files stay in the temporary root.
"""
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys

SOURCE = Path(__file__).resolve().parents[2] / '.bin/framework-sources/AstrBot'
REVISION = '9441402872f65fffc35685b0ead5bd3337033cb2'
assert subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip() == REVISION
subprocess.run(['git', '-C', str(SOURCE), 'diff', 'HEAD', '--exit-code', '--quiet'], check=True)
sys.path.insert(0, str(SOURCE))
os.environ['ASTRBOT_DISABLE_METRICS'] = '1'

from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain
from astrbot.core.config import VERSION
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import AiocqhttpAdapter

report = Path(os.environ['FRAMEWORK_REPORT'])


async def main():
    queue = asyncio.Queue()
    adapter = AiocqhttpAdapter({'id': 'synthetic-platform', 'ws_reverse_host': '127.0.0.1',
        'ws_reverse_port': int(os.environ['FRAMEWORK_PORT']),
        'ws_reverse_token': 'synthetic-framework-token'}, {}, queue)
    handled = []

    async def consume():
        while True:
            event = await queue.get()
            text = event.message_str.strip()
            if text not in ('/private-ping', '/group-ping'):
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
            chain = ([At(qq=event.get_sender_id())] if group else []) + [Plain(text='pong:' + text)]
            await event.send(MessageChain(chain))
            handled.append({'kind': 'group' if group else 'private',
                'user_id': int(event.get_sender_id()), 'self_id': int(message.self_id),
                'to_me': to_me, 'member_lookup_verified': group,
                'message_id': int(message.message_id)})
            report.write_text(json.dumps({'framework': 'astrbot',
                'scope': 'actual platform adapter, message conversion and send; local consumer replaces app pipeline',
                'source_revision': REVISION, 'versions': {'AstrBot': VERSION,
                    **{name: importlib.metadata.version(name) for name in ['aiocqhttp', 'quart', 'hypercorn']}},
                'metrics_disabled': True, 'handled': handled}, indent=2))

    consumer = asyncio.create_task(consume())
    try:
        await adapter.run()
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await adapter.terminate()


if __name__ == '__main__':
    asyncio.run(main())
