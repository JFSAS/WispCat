"""Actual NoneBot application/driver and plugin lifecycle, no provider/model."""
import importlib.metadata
import base64
import json
import os
from pathlib import Path
import sys

import nonebot
from nonebot import on_message
from nonebot.adapters.onebot.v11 import Adapter, Bot, MessageEvent, MessageSegment

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'framework'))
from stream_probe import probe
from media_probe import probe as media_probe
from crash_reply import source_for
from receipt_reply import enabled as receipt_enabled,request_for as receipt_request,completion as receipt_completion

report = Path(os.environ['FRAMEWORK_REPORT'])
nonebot.init(driver='~fastapi', host='127.0.0.1', port=int(os.environ['FRAMEWORK_PORT']),
             log_level='WARNING', onebot_access_token='synthetic-framework-token',
             onebot_v11_access_token='synthetic-framework-token')
driver = nonebot.get_driver()
driver.register_adapter(Adapter)
state = {'framework': 'nonebot', 'startup': False, 'shutdown': False,
         'api_timeout_seconds': float(driver.config.api_timeout),
         'connected': [], 'disconnected': [], 'handled': [],
         'versions': {name: importlib.metadata.version(name) for name in
                      ['nonebot2', 'nonebot-adapter-onebot', 'fastapi', 'pydantic']}}


def save():
    pending = report.with_suffix('.tmp')
    pending.write_text(json.dumps(state, indent=2) + '\n')
    pending.replace(report)


@driver.on_startup
async def startup():
    state['startup'] = True
    save()


@driver.on_shutdown
async def shutdown():
    state['shutdown'] = True
    state['bots_at_shutdown'] = len(nonebot.get_bots())
    save()


@driver.on_bot_connect
async def connected(bot: Bot):
    state['connected'].append(bot.self_id)
    save()


@driver.on_bot_disconnect
async def disconnected(bot: Bot):
    state['disconnected'].append(bot.self_id)
    save()


matcher = on_message(priority=1, block=True)


@matcher.handle()
async def reply(bot: Bot, event: MessageEvent):
    text = event.get_plaintext().strip()
    crash_source = source_for(text)
    receipt = receipt_enabled(text)
    media = [part for part in event.message if part.type in ('image','file')]
    if not media and not crash_source and not receipt and text not in ('/private-ping', '/group-ping'):
        return
    login, status = await bot.get_login_info(), await bot.get_status()
    assert int(login['user_id']) == int(bot.self_id)
    assert status['online'] and status['good']
    upload = await probe(bot.call_api, event.self_id, event.message_id)
    group = event.message_type == 'group'
    if group:
        member = await bot.get_group_member_info(group_id=event.group_id, user_id=event.user_id)
        assert member['user_id'] == event.user_id and member['wechat_roster_complete'] is False
        assert event.is_tome()
    extra = {}
    if receipt:
        params,record=receipt_request(text,event.self_id,event.user_id,event.sender.nickname)
        state['receipt_attempt']={'command':text,'record':record,'received_message_id':event.message_id}
        state['receipt_pending']=True
        save()
        try:
            if text=='/receipt-retry':
                state['receipt_lookup']=await bot.call_api('wechat_get_delivery_by_request',self_id=params['self_id'],wechat_request_id=params['wechat_request_id'])
                save()
            result=await bot.call_api('send_private_msg',**params)
        except Exception as error:
            state['receipt_pending']=False;state['receipt_error']=type(error).__name__
            state.setdefault('receipt_completions',[]).append(receipt_completion(text,'error',error));save()
            raise
        state['receipt_pending']=False
        state.setdefault('receipt_completions',[]).append(receipt_completion(text,'success'))
        state.setdefault('receipt_results',[]).append({'command':text,'record':record,'result':result})
        save()
        return
    elif crash_source:
        state['crash_attempt'] = {'self_id': event.self_id, 'user_id': event.user_id, 'received_message_id': event.message_id}
        save()
        await bot.send(event, MessageSegment('file', {'file': crash_source, 'name': 'interrupted.bin'}), at_sender=False)
        raise AssertionError('interrupted action unexpectedly completed')
    elif media:
        assert len(media) == 1
        part = media[0]
        content,extra = await media_probe(bot.call_api,event.self_id,{'type':part.type,'data':part.data},event.sender.nickname)
        if part.type == 'image':
            outgoing=MessageSegment.image('base64://'+base64.b64encode(content).decode())
        else:
            outgoing=MessageSegment('file',{'file':part.data['url'],'name':'reply.bin'})
        result=await bot.send(event,outgoing,at_sender=False)
    else:
        result = await bot.send(event, MessageSegment.text('pong:' + text), at_sender=group)
    state['handled'].append({'kind': event.message_type, 'self_id': event.self_id,
        'user_id': event.user_id, 'message_id': result['message_id'], 'to_me': event.is_tome(),
        'member_lookup_verified': group, 'stream_upload_verified': upload,
        'received_message_id':event.message_id,**extra})
    save()


if __name__ == '__main__':
    nonebot.run()
