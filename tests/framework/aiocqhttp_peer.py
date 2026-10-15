"""Actual aiocqhttp transport used by AstrBot's OneBot v11 adapter.

This validates that transport package, not AstrBot's full application/plugins.
"""
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path

from aiocqhttp import CQHttp, Event, MessageSegment

report = Path(os.environ['FRAMEWORK_REPORT'])
bot = CQHttp(use_ws_reverse=True, import_name='runtime-fixture',
             access_token='synthetic-framework-token', api_timeout_sec=5)
handled = []

@bot.on_message
async def reply(event: Event):
    segments = event.message
    text = ''.join(segment['data']['text'] for segment in segments
                   if segment['type'] == 'text').strip()
    if text not in ('/private-ping', '/group-ping'):
        return
    login = await bot.get_login_info(self_id=event.self_id)
    status = await bot.get_status(self_id=event.self_id)
    assert int(login['user_id']) == int(event.self_id)
    assert status['online'] and status['good']
    member_verified = False
    if event.message_type == 'group':
        member = await bot.get_group_member_info(group_id=event.group_id,
            user_id=event.user_id, no_cache=False, self_id=event.self_id)
        assert member['user_id'] == event.user_id and member['wechat_roster_complete'] is False
        member_verified = True
    result = await bot.send(event, MessageSegment.text('pong:' + text),
                            at_sender=event.message_type == 'group')
    handled.append({'kind': event.message_type, 'user_id': event.user_id,
                    'self_id': event.self_id,
                    'member_lookup_verified': member_verified,
                    'to_me': event.message_type == 'private' or any(
                        segment['type'] == 'at' and
                        str(segment['data']['qq']) == str(event.self_id)
                        for segment in segments),
                    'message_id': result['message_id']})
    report.write_text(json.dumps({'framework': 'aiocqhttp',
        'scope': 'AstrBot transport dependency, not full AstrBot application',
        'versions': {name: importlib.metadata.version(name)
                     for name in ['aiocqhttp', 'quart', 'hypercorn']},
        'handled': handled}, indent=2))

if __name__ == '__main__':
    asyncio.run(bot.run_task(host='127.0.0.1', port=int(os.environ['FRAMEWORK_PORT']),
                             debug=False))
