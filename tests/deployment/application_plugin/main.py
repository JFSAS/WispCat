"""A real loaded Star whose yielded result goes through RespondStage."""
import base64
import hashlib
import os
from pathlib import Path

from astrbot.api import sp,star
from astrbot.api.event import filter
from astrbot.api.message_components import At,Plain,Image,File
from media_probe import local_media_url,payload,label_from_sender
from stream_probe import probe
from application_report import read_report,write_report


class Main(star.Star):
    def __init__(self,context):
        super().__init__(context)
        self.context=context
        value=read_report();value['plugin_initialized']=True;write_report(value)

    @filter.on_astrbot_loaded()
    async def loaded(self):
        value=read_report();value['application_startup_hook']=True;write_report(value)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def reply(self,event):
        message=event.message_obj
        text='/'+event.message_str.strip().lstrip('/')
        parts=[part for part in message.message if isinstance(part,(Image,File))]
        if not parts and text not in ('/private-ping','/group-ping'):return
        adapter=next(item for item in self.context.platform_manager.get_insts() if item.meta().name=='aiocqhttp')
        group=bool(event.get_group_id())
        login=await adapter.bot.get_login_info(self_id=message.self_id)
        status=await adapter.bot.get_status(self_id=message.self_id)
        assert int(login['user_id'])==int(message.self_id) and status['online'] and status['good']
        if group:
            member=await adapter.bot.get_group_member_info(self_id=message.self_id,group_id=event.get_group_id(),user_id=event.get_sender_id())
            assert str(member['user_id'])==event.get_sender_id() and member['wechat_roster_complete'] is False
            assert event.is_at_or_wake_command
        streamed=await probe(lambda action,**params:getattr(adapter.bot,action)(**params),message.self_id,message.message_id)
        extra={}
        if parts:
            assert len(parts)==1
            part=parts[0]
            original=next(item for item in message.raw_message['message'] if item['type']==('image' if isinstance(part,Image) else 'file'))
            local_media_url(original['data']['url'])
            if isinstance(part,Image):
                localized=Path(part.url).resolve()
                assert localized.is_file() and localized.is_relative_to(Path(os.environ['ASTRBOT_ROOT']).resolve()/'data/temp')
                assert Path(part.file).resolve()==localized and Path(part.path).resolve()==localized
                kind='image';content=base64.b64decode(await part.convert_to_base64(),validate=True);chain=[Image.fromBytes(content)]
            else:
                kind='file';content=Path(await part.get_file()).read_bytes();chain=[File(name='reply.bin',url=original['data']['url'])]
            assert content==payload(kind,label_from_sender(message.sender.nickname))
            extra={'media':kind,'media_sha256':hashlib.sha256(content).hexdigest(),'received_file_id':str(original['data']['file']),'pipeline_image_localized':kind=='image'}
        else:
            chain=([At(qq=event.get_sender_id())] if group else [])+[Plain(text='pong:'+text)]
        event.set_extra('synthetic_application_probe',{'kind':'group' if group else 'private','user_id':int(event.get_sender_id()),'self_id':int(message.self_id),
            'to_me':not group or event.is_at_or_wake_command,'member_lookup_verified':group,'stream_upload_verified':streamed,
            'message_id':int(message.message_id),'received_message_id':int(message.message_id),**extra})
        yield event.chain_result(chain)

    @filter.after_message_sent()
    async def after_sent(self,event):
        row=event.get_extra('synthetic_application_probe')
        if not row:return
        key=f"synthetic_counter:{row['self_id']}:{row['user_id']}"
        count=await sp.global_get(key,0)+1
        await sp.global_put(key,count)
        row.update(persistent_user_count=count,after_message_sent_hook=True)
        value=read_report();value['handled'].append(row);write_report(value)

    async def terminate(self):
        value=read_report();value['plugin_terminated']=True;write_report(value)
