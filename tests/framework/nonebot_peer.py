"""Actual NoneBot driver/adapter, with a local test plugin and synthetic input."""
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path

import nonebot
import uvicorn
from nonebot import on_message
from nonebot.adapters.onebot.v11 import Adapter, Bot, MessageEvent, MessageSegment

report = Path(os.environ['FRAMEWORK_REPORT'])
token = 'synthetic-framework-token'
nonebot.init(driver='~fastapi', host='127.0.0.1', port=int(os.environ['FRAMEWORK_PORT']),
             log_level='WARNING', onebot_access_token=token, onebot_v11_access_token=token)
nonebot.get_driver().register_adapter(Adapter)
handled = []
matcher = on_message(priority=1, block=True)

@matcher.handle()
async def reply(bot: Bot, event: MessageEvent):
    text = event.get_plaintext().strip()
    if text not in ('/private-ping', '/group-ping'):
        return
    login = await bot.get_login_info()
    status = await bot.get_status()
    assert int(login['user_id']) == int(bot.self_id)
    assert status['online'] and status['good']
    member_verified = False
    if event.message_type == 'group':
        member = await bot.get_group_member_info(group_id=event.group_id, user_id=event.user_id)
        assert member['user_id'] == event.user_id and member['wechat_roster_complete'] is False
        member_verified = True
    result = await bot.send(event, MessageSegment.text('pong:' + text),
                            at_sender=event.message_type == 'group')
    handled.append({'kind': event.message_type, 'user_id': event.user_id,
                    'self_id': event.self_id, 'to_me': event.is_tome(),
                    'member_lookup_verified': member_verified,
                    'message_id': result['message_id']})
    report.write_text(json.dumps({'framework': 'nonebot',
        'versions': {name: importlib.metadata.version(name) for name in
                     ['nonebot2', 'nonebot-adapter-onebot', 'fastapi', 'pydantic']},
        'handled': handled}, indent=2))

if __name__ == '__main__':
    uvicorn.run(nonebot.get_app(), host='127.0.0.1', port=int(os.environ['FRAMEWORK_PORT']),
                log_level='warning', access_log=False)
