"""Official Python MCP SDK against the actual Go stdio/Host processes.

SDK process creation/termination is observed without replacing its transports,
schemas, session logic or responses. No production platform worker is started.
"""
import asyncio
from datetime import timedelta
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile

from mcp import ClientSession,StdioServerParameters,types
from mcp.client import stdio
from pydantic import AnyUrl

from source_capture import ROOT,load_checked
from stdio_probe import ADMIN,BOT,CONTROL,digest,wait_for
from offline_api import request_json
from probe_cleanup import close_host
from stall_fixture import StalledOneBot


async def run():
    output=ROOT/'.bin/mcp-sdk-result.json';output.unlink(missing_ok=True)
    capture,source_sha=load_checked()
    temp=Path(tempfile.mkdtemp(prefix='wechat-mcp-sdk-')).resolve()
    host=None;host_output=None;reserve=socket.socket();sdk_processes=[];escalations=[]
    original_create=stdio._create_platform_compatible_process
    original_terminate=stdio._terminate_process_tree
    async def observe_create(*args,**kwargs):
        process=await original_create(*args,**kwargs);sdk_processes.append(process);return process
    async def observe_terminate(process):
        escalations.append(process.pid);return await original_terminate(process)
    stdio._create_platform_compatible_process=observe_create
    stdio._terminate_process_tree=observe_terminate
    try:
        hashes={};private={}
        for name in ['mcp-host-fixture','mcp-stream-client']:
            source=ROOT/'.bin'/name;target=temp/name
            hashes[name]=digest(source);shutil.copyfile(source,target);target.chmod(0o500)
            assert digest(target)==hashes[name];private[name]=target
        state=temp/'state';state.mkdir(mode=0o700)
        marker=state/'synthetic-deployment.required';marker.write_text('wechat-runtime/synthetic-deployment/v1');marker.chmod(0o600)
        reserve.bind(('127.0.0.1',0))
        env={'PATH':os.environ['PATH'],'HOME':str(temp),'DEPLOYMENT_ADMIN_TOKEN':ADMIN,'DEPLOYMENT_BOT_TOKEN':BOT,'DEPLOYMENT_REVERSE_TOKEN':'synthetic-unused-reverse'}
        if 'LD_LIBRARY_PATH' in os.environ:env['LD_LIBRARY_PATH']=os.environ['LD_LIBRARY_PATH']
        log=temp/'host.log';host_output=log.open('w+')
        host=subprocess.Popen([str(private['mcp-host-fixture']),'--reverse',f'ws://127.0.0.1:{reserve.getsockname()[1]}/unused','--state-dir',str(state)],env=env,cwd=temp,stdout=host_output,stderr=subprocess.STDOUT)
        def addresses():
            lines=log.read_text().splitlines();values={label:next((line.removeprefix(label+': ').strip() for line in lines if line.startswith(label+': ')),'') for label in ['management','control']}
            return values if all(values.values()) else None
        points=await asyncio.to_thread(wait_for,addresses,host)
        assert request_json(points['control']+'/fixture/identity',CONTROL)['source_manifest_sha256']==source_sha
        rows=request_json(points['management']+'/accounts',ADMIN)
        endpoints={row['name']:'http://'+row['onebot_address'] for row in rows}
        uploads=temp/'uploads';uploads.mkdir(mode=0o700)
        downloads=temp/'downloads';read_dir=temp/'read-must-not-exist'
        base=dict(env,WECHAT_MCP_TOKEN=BOT,WECHAT_MCP_ENDPOINT=endpoints['synthetic-a'],WECHAT_MCP_UPLOAD_ROOT=str(uploads))
        negotiated=[];files=[];checks=[];discovery={}

        async def tool(session,name,action,params=None,execution=None,failed=False):
            args={'action':action,'params':params or {}}
            if execution is not None:args['execution']=execution
            response=await session.call_tool(name,args)
            assert response.isError==failed,response
            assert len(response.content)==1 and response.content[0].type=='text'
            return json.loads(response.content[0].text)

        async def session_check(mode,endpoint,download_dir,callback):
            environment=dict(base,WECHAT_MCP_MODE=mode,WECHAT_MCP_ENDPOINT=endpoint,WECHAT_MCP_STREAM_DIR=str(download_dir))
            parameters=StdioServerParameters(command=str(private['mcp-stream-client']),env=environment,cwd=str(temp))
            before=len(sdk_processes)
            async with stdio.stdio_client(parameters) as (reader,writer):
                async with ClientSession(reader,writer,read_timeout_seconds=timedelta(seconds=15)) as session:
                    initialized=await session.initialize();negotiated.append(initialized.protocolVersion)
                    await session.send_ping()
                    await callback(session)
            assert len(sdk_processes)==before+1 and sdk_processes[-1].returncode==0

        async def docs(session):
            tools=await session.list_tools();assert {tool.name for tool in tools.tools}=={'list_actions','get_action','search_actions','list_categories'}
            action=await session.call_tool('get_action',{'name':'upload_file_stream'})
            assert not action.isError and json.loads(action.content[0].text)['streaming']
            resources=await session.list_resources();assert str(resources.resources[0].uri)=='wechat://onebot/actions'
            data=await session.read_resource(AnyUrl('wechat://onebot/actions'))
            async def catalog_call(name,arguments):
                value=await session.call_tool(name,arguments)
                assert not value.isError and len(value.content)==1 and value.content[0].type=='text'
                return json.loads(value.content[0].text)
            all_actions=await catalog_call('list_actions',{})
            assert isinstance(all_actions,list) and len(all_actions)==22
            assert json.loads(data.contents[0].text)==all_actions
            categories=await catalog_call('list_categories',{})
            assert sum(item['count'] for item in categories)==len(all_actions)
            assert [item['category'] for item in categories]==sorted(item['category'] for item in categories)
            found=await catalog_call('search_actions',{'query':'  GROUP_MEMBER  ','category':'groups'})
            assert found['count']==1 and found['actions'][0]['name']=='get_group_member_info'
            assert found['actions'][0]['category']=='groups' and 'input_schema' not in found['actions'][0]
            group=await catalog_call('get_action',{'name':found['actions'][0]['name']})
            assert group['category']=='groups' and set(group['input_schema']['required'])=={'group_id','user_id'}
            streams=await catalog_call('list_actions',{'category':'streams'})
            assert streams and all(item['category']=='streams' for item in streams)
            assert (await catalog_call('search_actions',{'query':'delete_msg'}))=={'count':0,'actions':[]}
            for invalid_args in [{'query':'stream','unexpected':True},{'QUERY':'stream'},{'query':'absent','QUERY':'stream'}]:
                invalid=await session.call_tool('search_actions',invalid_args);assert invalid.isError
            for item in tools.tools:
                assert item.annotations.readOnlyHint and not item.annotations.openWorldHint and item.inputSchema['additionalProperties'] is False
            discovery.update(action_count=len(all_actions),categories=categories,tools=sorted(item.name for item in tools.tools),legacy_full_array=True,compact_search=True,resource_matches_full_records=True,invalid_arguments_rejected=True,unsupported_native_action_not_advertised=True)
            checks.append('initialize/ping/four read-only catalog tools, categories, compact search, full schema/resource and invalid-argument validation')
        await session_check('docs',endpoints['synthetic-a'],temp/'docs-must-not-exist',docs)
        assert not (temp/'docs-must-not-exist').exists()

        async def read(session):
            tools=await session.list_tools();assert 'invoke_action' not in {tool.name for tool in tools.tools}
            status=await tool(session,'query_action','get_status');assert status['data']['good']
            await tool(session,'query_action','send_private_msg',{'user_id':1,'message':'synthetic denied'},failed=True)
            await tool(session,'query_action','download_file_stream',{'file':'synthetic'},failed=True)
            assert not read_dir.exists();checks.append('read/write and local-file gates')
        await session_check('read',endpoints['synthetic-a'],read_dir,read)

        reference=''
        expected_body=b'official MCP SDK fixture\n'*32000
        outside=temp/'outside';outside.write_bytes(b'synthetic outside-root content')
        async def write(session):
            nonlocal reference
            tools=await session.list_tools();invoke=next(tool for tool in tools.tools if tool.name=='invoke_action')
            assert 'execution' in invoke.inputSchema['properties']
            login=await tool(session,'query_action','get_login_info');self_id=login['data']['user_id']
            body=expected_body
            source=uploads/'input.bin';source.write_bytes(body);source.chmod(0o600)
            uploaded=await tool(session,'invoke_action','upload_file_stream',{'self_id':self_id,'file_retention':0},{'input_file':str(source)})
            assert uploaded['terminal'] and uploaded['location']=='runtime_temporary_reference'
            reference=uploaded['file_path']
            downloaded=await tool(session,'invoke_action','download_file_stream',{'file':reference,'self_id':self_id,'chunk_size':32768})
            path=Path(downloaded['file_path']);assert path.parent==downloads and path.read_bytes()==body
            assert downloaded['sha256']==uploaded['sha256']==hashlib.sha256(body).hexdigest()
            files.append({'path':path,'bytes':len(body),'sha256':downloaded['sha256'],'frames':downloaded['frame_count']})
            summary=await tool(session,'query_action','test_download_stream');assert summary['frame_count']==11
            await tool(session,'invoke_action','upload_file_stream',None,{'input_file':str(temp/'outside')},failed=True)
            checks.append('automatic upload/validated download/non-file stream/result schemas')
        await session_check('write',endpoints['synthetic-a'],downloads,write)

        async def reopen(session):
            assert all(digest(item['path'])==item['sha256'] for item in files)
            checks.append('graceful SDK context close, directory owner reopen and retained file')
        await session_check('write',endpoints['synthetic-a'],downloads,reopen)

        async def other(session):
            await tool(session,'invoke_action','download_file_stream',{'file':reference},failed=True)
            checks.append('account-scoped reference refusal')
        await session_check('write',endpoints['synthetic-b'],temp/'other-downloads',other)
        async def verify_owner_then_clean(session):
            still_valid=await tool(session,'invoke_action','download_file_stream',{'file':reference})
            assert Path(still_valid['file_path']).read_bytes()==expected_body
            await tool(session,'invoke_action','clean_stream_temp_file')
            checks.append('owner reference still valid after sibling refusal, then explicit cleanup')
        await session_check('write',endpoints['synthetic-a'],downloads,verify_owner_then_clean)
        cancelled_dir=temp/'cancelled-downloads'
        with StalledOneBot() as stall:
            async def cancel_in_flight(session):
                # Read-only observation of this pinned SDK's next ID addresses
                # the actual public cancellation notification; no ID or stream
                # assignment, parser or response is replaced.
                request_id=session._request_id
                task=asyncio.create_task(tool(session,'invoke_action','download_file_stream',{'file':'synthetic-stall'}))
                try:
                    assert await asyncio.to_thread(stall.started.wait,5)
                    await asyncio.wait_for(session.send_ping(),3)
                    login=await asyncio.wait_for(tool(session,'query_action','get_login_info'),3)
                    assert login['data']['user_id']==123
                    await session.send_notification(types.ClientNotification(types.CancelledNotification(params=types.CancelledNotificationParams(requestId=request_id))))
                    assert await asyncio.to_thread(stall.cancelled.wait,5)
                    for _ in range(100):
                        if not list(cancelled_dir.glob('*.part')):break
                        await asyncio.sleep(.05)
                    assert not list(cancelled_dir.glob('*.part')) and not list(cancelled_dir.glob('*.bin'))
                    assert not task.done(),'cancelled request emitted a late result'
                    checks.append('concurrent ping/action while download stalls; actual cancellation notification closes HTTP and removes partial without a late response')
                finally:
                    task.cancel();await asyncio.gather(task,return_exceptions=True)
            await session_check('write',stall.url,cancelled_dir,cancel_in_flight)
        async def cancelled_owner_reopen(session):
            assert not list(cancelled_dir.glob('*.part')) and not list(cancelled_dir.glob('*.bin'))
            status=await tool(session,'query_action','get_status');assert status['data']['good']
            checks.append('cancelled file owner reopens without partials or lock leak')
        await session_check('write',endpoints['synthetic-a'],cancelled_dir,cancelled_owner_reopen)
        assert not escalations,escalations
        for row in request_json(points['management']+'/accounts',ADMIN):
            for key in ['auth_calls','sync_calls','send_calls','contact_list_calls','contact_brief_calls']:assert row['status'][key]==0
        host.send_signal(signal.SIGTERM);host.wait(timeout=10);assert host.returncode==0
        assert load_checked()[1]==source_sha
        assert all(digest(private[name])==hashes[name] for name in private)
        result={'result':'pass','sdk':'Official modelcontextprotocol Python SDK','sdk_version':importlib.metadata.version('mcp'),
            'scope':'Actual ClientSession/stdio transport/schema validation against Go MCP and separate Host/OneBot synthetic account fixture',
            'checks':checks,'catalog_discovery':discovery,'negotiated_protocol_versions':negotiated,'sdk_processes':len(sdk_processes),'sdk_process_exit_codes':[process.returncode for process in sdk_processes],
            'sdk_termination_escalations':len(escalations),'instrumentation':'Transparent observation of actual SDK subprocess create/termination; no transport/session/schema/response replacement',
            'request_dispatch':{'concurrent_ping_and_action_during_stalled_download':True,'cancellation_notification_closed_http_and_removed_partial':True,'cancelled_owner_reopened':True,'request_id_observation':'Read only next-ID counter of pinned SDK to address the public cancellation notification'},
            'files':[{'bytes':item['bytes'],'sha256':item['sha256'],'frames':item['frames']} for item in files],
            'mcp_source_path':'reports/mcp-source-'+source_sha+'.json','mcp_source_sha256':source_sha,'runtime_input_files':len(capture['runtime_inputs']),
            'binaries_sha256':hashes,'binary_snapshot':'private0500 executable copies checked before/after',
            'live_wechat_or_model_requests':0,'native_protocol_session_started':False,
            'limits':['Only the recorded SDK version/platform','At most8 active tools per session; local file operations remain serial; cancellation cannot undo committed runtime/platform effects','16MiB native ceiling; no live platform or model operation']}
    finally:
        stdio._create_platform_compatible_process=original_create;stdio._terminate_process_tree=original_terminate
        close_host(host,host_output,reserve,temp)
    output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'result':'pass','official_mcp_sdk':result['sdk_version'],'sdk_processes':result['sdk_processes'],'termination_escalations':0}))


if __name__=='__main__':asyncio.run(run())
