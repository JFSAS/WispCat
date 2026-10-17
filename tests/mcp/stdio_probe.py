"""Actual MCP stdio process and actual Host/Service/OneBot transfer storage.

Only disposable synthetic accounts are admitted. Run under loopback enforcement.
The Python client speaks MCP JSON-RPC; it is not a third-party MCP SDK test.
"""
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tools'))
from offline_api import request_json
from source_capture import load_checked

ADMIN='synthetic-deployment-admin-token'
BOT='synthetic-deployment-onebot-token'
CONTROL='synthetic-deployment-control-token'


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def wait_for(callback,process,timeout=15):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        assert process.poll() is None,process.returncode
        try:
            value=callback()
            if value:return value
        except (OSError,ValueError,KeyError):pass
        time.sleep(.03)
    raise TimeoutError('synthetic Host did not become ready')


class MCP:
    def __init__(self,binary,env,logs,processes):
        output=tempfile.TemporaryFile(mode='w+')
        logs.append(output)
        self.process=subprocess.Popen([str(binary)],env=env,cwd=env['HOME'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=output,text=True,bufsize=1)
        processes.append(self.process);self.count=0

    def send(self,method,params=None):
        self.count+=1
        self.process.stdin.write(json.dumps({'jsonrpc':'2.0','id':self.count,'method':method,'params':params or {}})+'\n')
        self.process.stdin.flush()
        return self.count

    def rpc(self,method,params=None):
        number=self.send(method,params)
        assert select.select([self.process.stdout],[],[],10)[0],'MCP response timeout'
        line=self.process.stdout.readline((2<<20)+1)
        assert line and len(line)<=2<<20,'MCP response absent/oversized'
        response=json.loads(line)
        assert response['id']==number and 'error' not in response,response
        return response['result']

    def tool(self,name,action,params=None,execution=None,failed=False):
        args={'action':action,'params':params or {}}
        if execution is not None:args['execution']=execution
        result=self.rpc('tools/call',{'name':name,'arguments':args})
        assert result['isError']==failed,result
        return json.loads(result['content'][0]['text'])

    def initialize(self):
        result=self.rpc('initialize',{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'synthetic-stdio-probe','version':'1'}})
        assert result['protocolVersion']=='2025-06-18'
        return self.rpc('tools/list')['tools']

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        self.process.wait(timeout=8)
        assert self.process.returncode==0,self.process.returncode
        self.process.stdin.close();self.process.stdout.close()


def run():
    output=ROOT/'.bin/mcp-stdio-result.json';output.unlink(missing_ok=True)
    capture,source_sha=load_checked()
    binaries={name:ROOT/'.bin'/name for name in ['mcp-host-fixture','mcp-stream-client']}
    hashes={name:digest(path) for name,path in binaries.items()}
    logs=[];processes=[];release=threading.Event();stall=None;thread=None
    temporary=tempfile.TemporaryDirectory(prefix='wispcat-mcp-stdio-')
    try:
        temp=Path(temporary.name).resolve()
        private={}
        for name,path in binaries.items():
            target=temp/name;shutil.copyfile(path,target);target.chmod(0o500)
            assert digest(target)==hashes[name];private[name]=target
        state=temp/'state';state.mkdir(mode=0o700)
        marker=state/'synthetic-deployment.required';marker.write_text('wechat-runtime/synthetic-deployment/v1');marker.chmod(0o600)
        env={'PATH':os.environ['PATH'],'HOME':str(temp),'PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1',
            'DEPLOYMENT_ADMIN_TOKEN':ADMIN,'DEPLOYMENT_BOT_TOKEN':BOT,'DEPLOYMENT_REVERSE_TOKEN':'synthetic-unused-reverse'}
        if 'LD_LIBRARY_PATH' in os.environ:env['LD_LIBRARY_PATH']=os.environ['LD_LIBRARY_PATH']
        # Reserve an owned non-listening socket so reverse retries can only
        # be refused locally, without touching an unrelated process.
        reserved=socket.socket();reserved.bind(('127.0.0.1',0))
        host_log=temp/'host.log'
        host_output=host_log.open('w+');logs.append(host_output)
        host=subprocess.Popen([str(private['mcp-host-fixture']),'--reverse',f'ws://127.0.0.1:{reserved.getsockname()[1]}/unused','--state-dir',str(state)],env=env,cwd=temp,stdout=host_output,stderr=subprocess.STDOUT)
        processes.append(host)
        def addresses():
            lines=host_log.read_text().splitlines()
            values={name:next((line.removeprefix(name+': ').strip() for line in lines if line.startswith(name+': ')),'') for name in ['management','control']}
            return values if all(values.values()) else None
        points=wait_for(addresses,host)
        assert request_json(points['control']+'/fixture/identity',CONTROL)['source_manifest_sha256']==source_sha
        rows=wait_for(lambda:request_json(points['management']+'/accounts',ADMIN),host)
        assert [row['name'] for row in rows]==['synthetic-a','synthetic-b']
        endpoint={row['name']:'http://'+row['onebot_address'] for row in rows}
        uploads=temp/'uploads';uploads.mkdir(mode=0o700)
        downloads=temp/'downloads'
        base=dict(env,WISPCAT_MCP_ENDPOINT=endpoint['synthetic-a'],WISPCAT_MCP_TOKEN=BOT,WISPCAT_MCP_MODE='write',WISPCAT_MCP_UPLOAD_ROOT=str(uploads),WISPCAT_MCP_STREAM_DIR=str(downloads),WISPCAT_MCP_MAX_STREAM_BYTES=str(2<<20))
        mcp=MCP(private['mcp-stream-client'],base,logs,processes)
        tools=mcp.initialize()
        invoke=next(tool for tool in tools if tool['name']=='invoke_action')
        assert 'execution' in invoke['inputSchema']['properties']
        body=b'synthetic MCP transfer\n'*35000
        image=base64.b64decode(json.loads((ROOT/'internal/service/deployment-media.json').read_text())['image_base64'],validate=True)
        received=[];references=[]
        for filename,content,action in [('fixture.bin',body,'download_file_stream'),('fixture.png',image,'download_file_image_stream')]:
            source=uploads/filename;source.write_bytes(content);source.chmod(0o600)
            uploaded=mcp.tool('invoke_action','upload_file_stream',{'file_retention':0},{'input_file':str(source)})
            assert uploaded['terminal'] and uploaded['location']=='runtime_temporary_reference'
            assert uploaded['sha256']==hashlib.sha256(content).hexdigest() and uploaded['file_size']==len(content)
            references.append(uploaded['file_path'])
            mcp.tool('query_action',action,{'file':uploaded['file_path']},failed=True)
            downloaded=mcp.tool('invoke_action',action,{'file':uploaded['file_path']})
            path=Path(downloaded['file_path'])
            assert downloaded['terminal'] and downloaded['location']=='mcp_host' and path.parent==downloads
            assert path.read_bytes()==content and downloaded['sha256']==uploaded['sha256']
            received.append({'path':path,'sha256':downloaded['sha256'],'bytes':len(content),'frames':downloaded['frame_count']})
        summary=mcp.tool('query_action','test_download_stream')
        assert summary['frame_count']==11
        # A concurrent process cannot share the download directory owner.
        locked=subprocess.run([str(private['mcp-stream-client'])],env=base,cwd=temp,input='',capture_output=True,text=True,timeout=8)
        assert locked.returncode==1 and 'mcp_stream_directory_in_use' in locked.stderr
        mcp.stop()
        reopened=MCP(private['mcp-stream-client'],base,logs,processes);reopened.initialize()
        for item in received:assert digest(item['path'])==item['sha256']
        other_env=dict(base,WISPCAT_MCP_ENDPOINT=endpoint['synthetic-b'],WISPCAT_MCP_STREAM_DIR=str(temp/'other-downloads'))
        other=MCP(private['mcp-stream-client'],other_env,logs,processes);other.initialize()
        other.tool('invoke_action','download_file_stream',{'file':references[0]},failed=True)
        assert not list((temp/'other-downloads').glob('*.bin')) and not list((temp/'other-downloads').glob('*.part'))
        other.stop()
        read_dir=temp/'read-must-not-exist'
        read=MCP(private['mcp-stream-client'],dict(base,WISPCAT_MCP_MODE='read',WISPCAT_MCP_STREAM_DIR=str(read_dir)),logs,processes)
        assert 'invoke_action' not in {tool['name'] for tool in read.initialize()}
        read.tool('query_action','download_file_stream',{'file':references[0]},failed=True)
        read.tool('query_action','upload_file_stream',failed=True)
        assert not read_dir.exists()
        read.stop()
        reopened.tool('invoke_action','clean_stream_temp_file')
        statistics=request_json(points['management']+'/accounts/synthetic-a/streams',ADMIN)
        assert statistics['file_count']==statistics['reserved_bytes']==0
        reopened.stop()

        # Cancel an active HTTP body, then verify no partial or held owner.
        entered=threading.Event()
        class Stalled(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200);self.send_header('Content-Type','application/x-ndjson');self.end_headers()
                self.wfile.write(b'{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"stream","data_type":"file_info","file_size":1,"file_name":"stalled.bin"}}\n');self.wfile.flush()
                entered.set();release.wait(timeout=15)
            def log_message(self,*_):pass
        stall=ThreadingHTTPServer(('127.0.0.1',0),Stalled);thread=threading.Thread(target=stall.serve_forever);thread.start()
        cancel_dir=temp/'cancel-downloads'
        cancel_env=dict(base,WISPCAT_MCP_ENDPOINT=f'http://127.0.0.1:{stall.server_port}',WISPCAT_MCP_STREAM_DIR=str(cancel_dir))
        cancelled=MCP(private['mcp-stream-client'],cancel_env,logs,processes);cancelled.initialize()
        cancelled.send('tools/call',{'name':'invoke_action','arguments':{'action':'download_file_stream','params':{'file':'synthetic'}}})
        assert entered.wait(timeout=5)
        cancelled.stop()
        assert not list(cancel_dir.glob('*.part')) and not list(cancel_dir.glob('*.bin'))
        recovered=MCP(private['mcp-stream-client'],dict(base,WISPCAT_MCP_STREAM_DIR=str(cancel_dir)),logs,processes)
        recovered.initialize();recovered.stop()
        release.set();stall.shutdown();stall.server_close();thread.join(timeout=5);assert not thread.is_alive();stall=None
        pressure_dir=temp/'pressure-downloads'
        pressure_env=dict(base,WISPCAT_MCP_STREAM_DIR=str(pressure_dir))
        pressure=MCP(private['mcp-stream-client'],pressure_env,logs,processes);pressure.initialize()
        for _ in range(32):pressure.send('tools/call',{'name':'list_actions','arguments':{}})
        assert select.select([pressure.process.stdout],[],[],5)[0]
        time.sleep(.2)
        pressure.stop()
        pressure_reopen=MCP(private['mcp-stream-client'],pressure_env,logs,processes)
        pressure_reopen.initialize();pressure_reopen.stop()
        invalid=[]
        for key,value in [('WISPCAT_MCP_MAX_STREAM_BYTES','0'),('WISPCAT_MCP_MAX_STREAM_BYTES',str((16<<20)+1)),('WISPCAT_MCP_MAX_STREAM_BYTES','+1'),('WISPCAT_MCP_TIMEOUT_MS','600001')]:
            bad=subprocess.run([str(private['mcp-stream-client'])],env=dict(base,**{key:value}),cwd=temp,input='',capture_output=True,text=True,timeout=8)
            assert bad.returncode==1 and 'invalid_mcp_transfer_config' in bad.stderr
            invalid.append(key+'='+value)
        final_accounts=request_json(points['management']+'/accounts',ADMIN)
        for row in final_accounts:
            for key in ['auth_calls','sync_calls','send_calls','contact_list_calls','contact_brief_calls']:assert row['status'][key]==0
            assert row['status']['connection']['disk_read']=='not_attempted'
        host.send_signal(signal.SIGTERM);host.wait(timeout=10);assert host.returncode==0
        reserved.close()
        assert all(digest(private[name])==hashes[name] for name in private)
        assert load_checked()[1]==source_sha
        result={'result':'pass','scope':'Actual wispcat-mcp stdio process, Python JSON-RPC probe and actual Host/Service/OneBot encrypted stream storage; synthetic account worker only',
            'third_party_mcp_client':False,'file_round_trips':[{'bytes':item['bytes'],'sha256':item['sha256'],'frames':item['frames']} for item in received],
            'completed_files_survive_mcp_reopen':True,'cross_account_reference_denied':True,'read_mode_created_no_directory':True,'concurrent_directory_owner_refused':True,
            'active_sigterm_cancelled_http_and_removed_partial':True,'idle_sigterm_exit_zero':True,'post_cancel_owner_reopen':True,'runtime_stream_cleanup_zero':True,
            'stdout_backpressure_sigterm_exit_zero':True,'stdout_backpressure_owner_reopen':True,
            'invalid_configurations_rejected':invalid,'all_success_path_processes_exit_zero':True,'no_forced_kill_in_success_path':True,
            'binaries_sha256':hashes,'binary_snapshot':'private0500 copies verified before/after execution','mcp_source_path':'.bin/reports/mcp-source-'+source_sha+'.json','mcp_source_sha256':source_sha,
            'runtime_input_files':len(capture['runtime_inputs']),'live_wechat_or_model_requests':0,'native_protocol_session_started':False,
            'limits':['16MiB native/SDK ceiling unchanged','Python protocol client only, not a third-party MCP SDK','No native platform session/CDN transport, full AstrBot pipeline,4GiB or long-duration capacity proof']}
    except Exception:
        for log in logs:log.flush();log.seek(0);sys.stderr.write(log.read()[-10000:])
        raise
    finally:
        release.set()
        if stall is not None:stall.shutdown();stall.server_close()
        if thread is not None:thread.join(timeout=5)
        for process in processes:
            if process.poll() is None:process.send_signal(signal.SIGTERM)
        for process in processes:
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        for log in logs:log.close()
        temporary.cleanup()

    output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'result':'pass','mcp_stdio_file_round_trips':2,'cross_account_denied':True,'active_and_idle_sigterm_exit_zero':True}))


if __name__=='__main__':run()
