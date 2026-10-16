"""Actual AstrBot InitialLoader/Core, plugin discovery and complete pipeline.

Providers and WebUI listening are disabled. Model metadata uses an owned
loopback fixture through a labelled endpoint-constant override; source is intact.
"""
import asyncio
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import threading

SOURCE=Path(os.environ['DEPLOYMENT_ASTRBOT_SOURCE'])
ROOT=Path(os.environ['ASTRBOT_ROOT'])
REPORT=Path(os.environ['FRAMEWORK_REPORT'])
REVISION='9441402872f65fffc35685b0ead5bd3337033cb2'
assert os.environ['DEPLOYMENT_ASTRBOT_REVISION']==REVISION
sys.path.insert(0,str(SOURCE));sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'framework'))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from application_audit import is_noop_sandbox_probe

requests=[];denied=[];local_probes=[]


def audit(event,args):
    if event=='socket.connect':
        address=args[1]
        if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):
            denied.append(event);raise PermissionError('offline application refuses external connection')
    if event=='socket.getaddrinfo' and args[0] not in ('127.0.0.1','::1','localhost',None):
        denied.append(event);raise PermissionError('offline application refuses external DNS')
    if event=='subprocess.Popen':
        seatbelt=sys.modules.get('astrbot.core.computer.process_sandbox.seatbelt')
        base=sys.modules.get('astrbot.core.computer.process_sandbox.base')
        def expected(workspace):
            return seatbelt.SeatbeltProcessSandbox().build_command(['/bin/sh','-c',':'],base.SandboxSpec(workspace=workspace))
        if seatbelt is not None and base is not None and is_noop_sandbox_probe(args,ROOT,expected):
            local_probes.append('upstream_noop_process_sandbox_capability_probe');return
        denied.append(event);raise PermissionError('offline application refuses other child processes')


class Metadata(BaseHTTPRequestHandler):
    def do_GET(self):
        assert self.path=='/models'
        requests.append(self.path)
        self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{}')
    def log_message(self,*_):pass


async def main():
    server=ThreadingHTTPServer(('127.0.0.1',0),Metadata)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    url=f'http://127.0.0.1:{server.server_port}'
    plugin=ROOT/'data/plugins/synthetic_application_probe';plugin.mkdir(parents=True,exist_ok=True)
    for source in (Path(__file__).parent/'application_plugin').iterdir():
        if source.is_file():shutil.copyfile(source,plugin/source.name)
    config={'disable_metrics':True,'dashboard':{'enable':False},'provider':[],'provider_sources':[],'provider_settings':{'enable':False},
        'platform':[{'id':'synthetic-application','type':'aiocqhttp','enable':True,'ws_reverse_host':'127.0.0.1','ws_reverse_port':int(os.environ['FRAMEWORK_PORT']),'ws_reverse_token':'synthetic-framework-token'}],
        't2i':False,'t2i_endpoint':url+'/text2img','platform_settings':{'enable_id_white_list':False,'rate_limit':{'time':60,'count':1000,'strategy':'discard'}},'wake_prefix':['/']}
    (ROOT/'data/cmd_config.json').write_text(json.dumps(config))
    REPORT.write_text(json.dumps({'framework':'astrbot-application','handled':[],'source_revision':REVISION,'versions':{'AstrBot':'4.29.0',**{name:importlib.metadata.version(name) for name in ['aiocqhttp','quart','hypercorn']}},
        'scope':'Actual InitialLoader/Core/plugin manager/EventBus/nine-stage pipeline/SQLite/adapter; no provider/model or dashboard listener'}))
    sys.addaudithook(audit)
    from astrbot.core import LogBroker,db_helper
    from astrbot.core.initial_loader import InitialLoader
    import astrbot.core.utils.llm_metadata as metadata
    metadata.LLM_METADATA_URLS=(url+'/models',)
    from application_report import read_report,write_report
    stopping=asyncio.Event();loop=asyncio.get_running_loop()
    for signum in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(signum,stopping.set)
    loader=InitialLoader(db_helper,LogBroker())
    application=asyncio.create_task(loader.start())
    try:
        for _ in range(400):
            if hasattr(loader,'dashboard_server'):break
            if application.done():await application;raise RuntimeError('AstrBot application failed during initialization')
            await asyncio.sleep(.05)
        assert hasattr(loader,'dashboard_server')
        core=loader.dashboard_server.core_lifecycle
        assert not core.plugin_manager.failed_plugin_dict
        assert len(core.provider_manager.provider_insts)==0
        stages=[[type(stage).__name__ for stage in scheduler.stages] for scheduler in core.pipeline_scheduler_mapping.values()]
        assert stages and all(len(names)==9 for names in stages),stages
        value=read_report();value.update(application_initialized=True,pipeline_stages=stages,metrics_disabled=True,providers_configured=0,dashboard_listener_started=False);write_report(value)
        waiter=asyncio.create_task(stopping.wait())
        finished,_=await asyncio.wait([application,waiter],return_when=asyncio.FIRST_COMPLETED)
        if application in finished:await application;raise RuntimeError('application exited before shutdown')
        for _ in range(100):
            if not core.event_bus._pending_tasks:break
            await asyncio.sleep(.05)
        assert not core.event_bus._pending_tasks
        platform_tasks=list(core.platform_manager._platform_tasks.values())
        assert platform_tasks
        application.cancel();await asyncio.wait_for(application,15)
        waiter.cancel();await asyncio.gather(waiter,return_exceptions=True)
        assert not core.event_bus._pending_tasks
        assert all(task.done() for task in core.curr_tasks)
        assert all(item.run.done() and item.wrapper.done() for item in platform_tasks)
        value=read_report();assert value['plugin_terminated'] and value['application_startup_hook']
        assert not denied,denied
        assert requests==['/models'],requests
        value.update(application_shutdown=True,adapter_shutdown=True,server_joined=True,pipeline_tasks_joined=True,
            metadata_endpoint_override='owned loopback /models; original update function executed',metadata_requests=len(requests),
            denied_network_or_child_operations=denied,local_noop_sandbox_probes=len(local_probes))
        write_report(value)
    finally:
        if not application.done():application.cancel();await asyncio.gather(application,return_exceptions=True)
        server.shutdown();server.server_close();thread.join(timeout=5)
        assert not thread.is_alive()


if __name__=='__main__':asyncio.run(main())
