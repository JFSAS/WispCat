"""Separate framework and actual Host processes with synthetic native facts.

Run only under enforced loopback isolation. No platform session loop is started.
"""
import hashlib
import asyncio
import urllib.parse
import httpx
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools'))
from offline_api import request_json
from source_capture import load_checked, freeze
from media_probe import payload,fetch,local_media_url,CORPUS,abandon
from crash_source import StalledSource,cancellation_window
from receipt_relay import ReceiptRelay
from receipt_reply import parameters as receipt_parameters, original_completion

ADMIN = 'synthetic-deployment-admin-token'
BOT = 'synthetic-deployment-onebot-token'
CONTROL = 'synthetic-deployment-control-token'
ACCOUNTS = ['synthetic-a', 'synthetic-b']


def wait_for(callback, processes, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f'deployment process exited: {process.returncode}')
        try:
            result = callback()
            if result:
                return result
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.05)
    raise TimeoutError('offline deployment did not reach its required state')


def run(framework,scenario='text'):
    assert framework in ('nonebot', 'astrbot','astrbot-application') and scenario in ('text','media','large-media','crash-media','receipt-loss','runtime-crash','listener-swap','configuration-swap')
    application=framework=='astrbot-application'
    configuration_swap=scenario=='configuration-swap'
    listener_swap=scenario in ('listener-swap','configuration-swap')
    crash=scenario=='crash-media'
    runtime_crash=scenario=='runtime-crash'
    loss=scenario in ('receipt-loss','runtime-crash')
    assert not (crash or loss) or framework in ('nonebot','astrbot-application')
    large=scenario in ('large-media','crash-media','receipt-loss','runtime-crash')
    os.environ['DEPLOYMENT_MEDIA_PROFILE']='large' if large else 'small'
    media_kinds=['text','image','file'] if scenario!='text' else ['text']
    cases=[(kind,media) for media in media_kinds for kind in ['private','group']]
    per_account=len(cases)
    per_round=per_account*len(ACCOUNTS)
    source, source_sha = load_checked()
    output = ROOT/('.bin/deployment-configuration-swap-results' if configuration_swap else '.bin/deployment-listener-swap-results' if listener_swap else '.bin/deployment-runtime-crash-results' if runtime_crash else '.bin/deployment-receipt-results' if loss else '.bin/deployment-crash-results' if crash else '.bin/deployment-large-results' if large else '.bin/deployment-application-results' if application else '.bin/deployment-media-results' if scenario=='media' else '.bin/deployment-results')/(framework+'.json')
    output.parent.mkdir(exist_ok=True)
    output.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix='wechat-deployment-') as directory:
        temp = Path(directory)
        frozen, astrbot_source, binary, binary_sha = freeze(temp, source)
        state_root = temp / 'state'
        state_root.mkdir(mode=0o700)
        marker = state_root / 'synthetic-deployment.required'
        marker.write_text('wechat-runtime/synthetic-deployment/v1')
        marker.chmod(0o600)
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        peer_report = temp / 'peer.json'
        env = {'PATH': os.environ['PATH'], 'HOME': str(temp), 'PYTHONNOUSERSITE': '1',
            'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUNBUFFERED': '1', 'DEPLOYMENT_MEDIA_PROFILE':'large' if large else 'small',
            'FRAMEWORK_PORT': str(port), 'FRAMEWORK_REPORT': str(peer_report),
            'ASTRBOT_ROOT': str(temp), 'ASTRBOT_DISABLE_METRICS': '1',
            'DEPLOYMENT_ADMIN_TOKEN': ADMIN, 'DEPLOYMENT_BOT_TOKEN': BOT,
            'DEPLOYMENT_REVERSE_TOKEN': 'synthetic-framework-token'}
        (temp/'system-temp').mkdir(mode=0o700)
        env.update(TMPDIR=str(temp/'system-temp'),ASTRBOT_DASHBOARD_INITIAL_PASSWORD='OfflineFixture-Only2026!',UV_OFFLINE='1',PIP_NO_INDEX='1')
        env.update(DEPLOYMENT_ASTRBOT_SOURCE=str(astrbot_source),DEPLOYMENT_ASTRBOT_REVISION=source['astrbot_revision'])
        bound = json.loads(subprocess.check_output([str(binary),'--metadata'],env=env,text=True))
        assert bound['source_manifest_sha256'] == source_sha
        peer_source = frozen/'tests/deployment'/('astrbot_application_peer.py' if application else framework+'_peer.py')
        websocket = f'ws://127.0.0.1:{port}' + ('/onebot/v11/ws' if framework == 'nonebot' else '/ws')
        processes, logs, observations = [], [], []
        starts = {'peer': 0, 'host': 0}
        stalled = None
        relay = None

        def start(kind):
            starts[kind] += 1
            output = (temp / f'{kind}-{starts[kind]}.log').open('w+')
            logs.append(output)
            assert hashlib.sha256(binary.read_bytes()).hexdigest() == binary_sha
            argv = [sys.executable, str(peer_source)] if kind == 'peer' else [str(binary), '--reverse', websocket, '--state-dir', str(state_root)]
            process = subprocess.Popen(argv, cwd=temp, env=env, stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process, Path(output.name)

        def stop(process, peer_kind=None):
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=15)
            processes.remove(process)
            if peer_kind == 'nonebot':
                result = json.loads(peer_report.read_text())
                assert result['startup'] and result['shutdown'] and result['bots_at_shutdown'] == 0
                assert process.returncode in (0, -signal.SIGTERM), process.returncode
                return result
            if peer_kind in ('astrbot','astrbot-application'):
                result=json.loads(peer_report.read_text())
                assert result['adapter_shutdown'] and result['server_joined']
                if application:
                    assert result['application_initialized'] and result['application_startup_hook'] and result['application_shutdown']
                    assert result['plugin_initialized'] and result['plugin_terminated'] and result['pipeline_tasks_joined']
                    assert result['denied_network_or_child_operations']==[]
                    assert result['providers_configured']==0 and result['dashboard_listener_started'] is False
                assert process.returncode == 0, process.returncode
                return result
            assert process.returncode == 0, process.returncode

        def kill_owned(process):
            assert process in processes and process.poll() is None
            process.kill()
            process.wait(timeout=5)
            assert process.returncode == -signal.SIGKILL, process.returncode
            processes.remove(process)

        try:
            if crash:
                stalled = StalledSource()
                env['DEPLOYMENT_CRASH_SOURCE'] = stalled.url
            if loss:
                relay = ReceiptRelay(websocket)
                websocket = relay.url
                env.update(DEPLOYMENT_RECEIPT_LOSS='1',DEPLOYMENT_RECEIPT_RECORD=str(temp/'consumer-request.json'))
            peer, _ = start('peer')
            def listening():
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    return True
            wait_for(listening, processes)
            host_process, host_log = start('host')

            def addresses():
                lines = host_log.read_text().splitlines()
                values = {label: next((line.removeprefix(label + ': ').strip() for line in lines if line.startswith(label + ': ')), '') for label in ['management', 'control']}
                return values if all(values.values()) else None
            points = wait_for(addresses, processes)
            assert request_json(points['control']+'/fixture/identity',CONTROL)['source_manifest_sha256'] == source_sha

            def admin(path, method='GET', body=None):
                return request_json(points['management'] + path, ADMIN, method, body)

            def inspect(name):
                value = request_json(points['control'] + '/fixture/inspect?account=' + name, CONTROL)
                assert value['error'] == '', value
                return value

            def accounts():
                rows = admin('/accounts')
                for row in rows:
                    status = row['status']
                    assert status['connection']['disk_read'] == 'not_attempted'
                    for counter in (['auth_calls','sync_calls','contact_list_calls','contact_brief_calls']+(['send_calls'] if scenario=='text' else [])):
                        assert status[counter] == 0, status
                return rows

            def connected(names=ACCOUNTS):
                rows = accounts()
                relay_ready = True
                if relay is not None:
                    state=relay.snapshot();assert not state['failures'],state
                    relay_ready=len(state['ready_ids'])==len(names)
                return rows if relay_ready and sorted(row['name'] for row in rows) == sorted(names) and all(len(row['connections']) == 1 and row['connections'][0]['state'] == 'connected' for row in rows) else None

            def change_connections(enabled):
                for name in ACCOUNTS:
                    path = '/accounts/' + name + '/connections'
                    saved = admin(path)
                    plan = {'reverse': [{'name': 'framework', 'url': websocket, 'token_env': 'DEPLOYMENT_REVERSE_TOKEN', 'role': 'Universal'}] if enabled else [], 'http_post': []}
                    admin(path, 'POST', {'revision': saved['revision'], 'plan': plan})
                    admin(path + '/apply', 'POST', {})
                    result = admin(path)
                    assert result['applied'], result

            def exercise(phase, handled_count):
                before = {name: len(inspect(name)['sends']) for name in ACCOUNTS}
                for name in ACCOUNTS:
                    for kind,media in cases:
                        value={'account':name,'kind':kind,'text':'/'+kind+'-ping','mentioned':kind=='group'}
                        if media!='text':value['media']=('large_' if large else '')+media
                        request_json(points['control']+'/fixture/inject',CONTROL,'POST',value)
                        handled_count+=1
                        wait_for(lambda: peer_report.exists() and len(json.loads(peer_report.read_text())['handled'])==handled_count,processes)
                handled=json.loads(peer_report.read_text())['handled'][-per_round:]
                receipt_ids=[]
                for i,name in enumerate(ACCOUNTS):
                    group=handled[i*per_account:(i+1)*per_account]
                    assert len({item['self_id'] for item in group})==1
                    assert len({item['user_id'] for item in group})==1
                    for item,(kind,media) in zip(group,cases):
                        assert item['kind']==kind and item.get('media','text')==media
                        assert item['to_me'] and item['stream_upload_verified']
                        assert item['member_lookup_verified']==(kind=='group')
                        if media!='text':assert item['media_sha256']==hashlib.sha256(payload(media,name)).hexdigest()
                    if application:
                        for index,item in enumerate(group):
                            assert item['after_message_sent_hook'] and item['persistent_user_count']==len(observations)*per_account+index+1,item
                            if item.get('media')=='image':assert item['pipeline_image_localized']
                    wait_for(lambda:len(inspect(name)['sends'])==before[name]+per_account,processes)
                    sent=inspect(name)['sends'][before[name]:]
                    assert sorted((row['kind'],row.get('media','text')) for row in sent)==sorted(cases),sent
                    for row in sent:
                        assert row['synthetic_server_id']>0
                        media=row.get('media','text')
                        if media=='text':
                            assert row['text'].endswith('pong:/'+row['kind']+'-ping'),row
                            assert row['mentions']==(['fixture-friend-'+name] if row['kind']=='group' else []),row
                        else:
                            body=payload(media,name)
                            assert row['payload_bytes']==len(body) and row['payload_sha256']==hashlib.sha256(body).hexdigest(),row
                            assert row['encoded_bytes']>0 and len(row['encoded_sha256'])==64 and row['mentions']==[],row
                        receipt_ids.append(row['synthetic_server_id'])
                assert handled[0]['self_id']!=handled[per_account]['self_id']
                assert handled[0]['user_id']!=handled[per_account]['user_id']
                assert len(set(receipt_ids))==per_round
                targets={name:admin('/accounts/'+name+'/history')['cursor'] for name in ACCOUNTS}
                wait_for(lambda:all(row['connections'][0].get('cursor',0)>=targets[row['name']] for row in accounts()),processes)
                observed={'phase':phase,'handled':handled,'accounts':accounts(),'drivers':{name:inspect(name) for name in ACCOUNTS}}
                for row in observed['accounts']:
                    assert row['status']['send_calls']==observed['drivers'][row['name']]['synthetic_uploads']
                observations.append(observed)
                return handled

            def media_references_still_readable(records,expected_downloads):
                rows={row['name']:row for row in accounts()}
                for record in records:
                    name=record['account'];base='http://'+rows[name]['onebot_address']
                    result=request_json(base+'/get_msg',BOT,'POST',{'message_id':record['message_id']})
                    assert result['retcode']==0
                    part=next(part for part in result['data']['message'] if part['type']==record['media'])
                    assert str(part['data']['file'])==record['file_id']
                    url=part['data']['url'];local_media_url(url)
                    assert asyncio.run(fetch(url))==payload(record['media'],name)
                    other=next(row for label,row in rows.items() if label!=name)
                    denied=urllib.parse.urlparse(url)._replace(netloc=other['onebot_address']).geturl()
                    with httpx.Client(timeout=5,trust_env=False,follow_redirects=False) as client:
                        assert client.get(denied).status_code==409
                assert expected_downloads=={name:inspect(name)['synthetic_downloads'] for name in ACCOUNTS}

            first_accounts = wait_for(connected, processes)
            topology = admin('/topology')['saved']
            initial = exercise('initial', 0)
            old_messages = []
            for row, item in zip(first_accounts, [initial[0], initial[per_account]]):
                base = 'http://' + row['onebot_address']
                reply = request_json(base + '/get_msg', BOT, 'POST', {'message_id': item['message_id']})
                assert reply['retcode'] == 0, reply
                old_messages.append({'account': row['name'], 'id': item['message_id'], 'data': reply['data']})
            old_media=[]
            for i,name in enumerate(ACCOUNTS):
                for item in initial[i*per_account:(i+1)*per_account]:
                    if item.get('media'):
                        old_media.append({'account':name,'media':item['media'],'file_id':item['received_file_id'],'message_id':item['received_message_id']})
            interrupted_http_reads=[]
            if large:
                # Exercise client-side interruption after native caching has
                # completed. This is not a killed framework/CDN worker test.
                row=next(item for item in accounts() if item['name']==ACCOUNTS[0])
                records=[next(item for item in old_media if item['account']==ACCOUNTS[0] and item['media']==kind) for kind in ['image','file']]
                before_downloads=inspect(ACCOUNTS[0])['synthetic_downloads']
                for record in records:
                    result=request_json('http://'+row['onebot_address']+'/get_msg',BOT,'POST',{'message_id':record['message_id']})
                    part=next(item for item in result['data']['message'] if item['type']==record['media'])
                    url=part['data']['url'];prefixes=[]
                    for _ in range(16):
                        prefixes.append(asyncio.run(abandon(url)))
                    complete=asyncio.run(fetch(url));assert complete==payload(record['media'],ACCOUNTS[0])
                    status=request_json('http://'+row['onebot_address']+'/get_status',BOT)
                    assert status['retcode']==0 and status['data']['good']
                    assert inspect(ACCOUNTS[0])['synthetic_downloads']==before_downloads
                    interrupted_http_reads.append({'media':record['media'],'aborted_reads':len(prefixes),'prefix_bytes':prefixes,'complete_retry_bytes':len(complete),'sha256':hashlib.sha256(complete).hexdigest(),'extra_cdn_downloads':0})
            # Stop the actual host process. A zero exit requires account workers,
            # requests, reverse transports, archives and shared index to join.
            stop(host_process)
            host_process, host_log = start('host')
            points = wait_for(addresses, processes)
            assert request_json(points['control']+'/fixture/identity',CONTROL)['source_manifest_sha256'] == source_sha
            restarted_accounts = wait_for(connected, processes)
            host_reopen_downloads={name:inspect(name)['synthetic_downloads'] for name in ACCOUNTS}
            assert host_reopen_downloads==dict.fromkeys(ACCOUNTS,0),host_reopen_downloads
            media_references_still_readable(old_media,host_reopen_downloads)
            listener_swap_evidence = None
            if listener_swap:
                before_swap = {row['name']: row for row in accounts()}
                drivers_before_swap = {name: inspect(name) for name in ACCOUNTS}
                login_before_swap = {name: request_json('http://'+row['onebot_address']+'/get_login_info',BOT)['data'] for name,row in before_swap.items()}
                kept_peer_pid = peer.pid
                configuration_revision = None
                if configuration_swap:
                    configuration = admin('/configuration')
                    plan = configuration['saved']
                    for account in plan['accounts']:
                        index = ACCOUNTS.index(account['name'])
                        account['listen'] = before_swap[ACCOUNTS[1-index]]['onebot_address']
                    saved_configuration = admin('/configuration','POST',{'revision':configuration['revision'],'plan':plan})
                    assert saved_configuration['persistence']=='ready' and saved_configuration['saved']==plan
                    configuration_revision = saved_configuration['revision']
                    assert {row['name']:row for row in accounts()}==before_swap,'whole save activated runtime changes'
                else:
                    for index,name in enumerate(ACCOUNTS):
                        path = '/accounts/'+name+'/listener'
                        saved = admin(path)
                        plan = dict(saved['saved'],listen=before_swap[ACCOUNTS[1-index]]['onebot_address'])
                        admin(path,'POST',{'revision':saved['revision'],'plan':plan})
                preview = admin('/listeners')
                assert preview['applied'] is False
                applied = admin('/listeners/apply','POST',{'revision':preview['revision']})
                assert applied['applied'] and applied['revision']==preview['revision'],applied
                after_swap = {row['name']: row for row in accounts()}
                for index,name in enumerate(ACCOUNTS):
                    before,after = before_swap[name],after_swap[name]
                    assert after['onebot_address']==before_swap[ACCOUNTS[1-index]]['onebot_address']
                    assert after['connections']==before['connections'],'reverse connection was restarted by inbound handoff'
                    assert inspect(name)==drivers_before_swap[name],'listener handoff changed native driver work or generation'
                    login = request_json('http://'+after['onebot_address']+'/get_login_info',BOT)
                    assert login['retcode']==0 and login['data']==login_before_swap[name]
                media_references_still_readable(old_media,host_reopen_downloads)
                assert peer.pid==kept_peer_pid and peer.poll() is None
                listener_swap_evidence = {'before':before_swap,'after':after_swap,'revision':preview['revision'],
                    'same_framework_process':True,'reverse_connections_unchanged':True,'native_driver_facts_unchanged':True,
                    'account_ids_preserved':True,'old_media_ids_and_bytes_preserved':True,'extra_native_downloads':0}
                if configuration_swap:
                    # Reopen the actual Host after the whole configuration was
                    # saved, retaining the same external framework process.
                    stop(host_process)
                    host_process,host_log = start('host')
                    points = wait_for(addresses,processes)
                    reopened = wait_for(connected,processes)
                    configuration = admin('/configuration')
                    assert configuration['revision']==configuration_revision and configuration['persistence']=='ready'
                    assert {row['name']:row['onebot_address'] for row in reopened}=={name:row['onebot_address'] for name,row in after_swap.items()}
                    host_reopen_downloads={name:inspect(name)['synthetic_downloads'] for name in ACCOUNTS}
                    assert host_reopen_downloads==dict.fromkeys(ACCOUNTS,0)
                    media_references_still_readable(old_media,host_reopen_downloads)
                    assert peer.pid==kept_peer_pid and peer.poll() is None
                    listener_swap_evidence['whole_configuration']={'revision':configuration_revision,'persisted_after_host_reopen':True,'framework_process_retained_during_reopen':True,'all_saved_values_preserved':configuration['saved']==plan}
                    assert listener_swap_evidence['whole_configuration']['all_saved_values_preserved']
                restarted_accounts = accounts()
            after_host = exercise('host_reopen_and_configuration_swap' if configuration_swap else 'host_reopen_and_listener_swap' if listener_swap else 'host_reopen', per_round)
            for first, after in zip(initial, after_host):
                assert (first['self_id'], first['user_id']) == (after['self_id'], after['user_id'])
            for old in old_messages:
                row = next(row for row in restarted_accounts if row['name'] == old['account'])
                reply = request_json('http://' + row['onebot_address'] + '/get_msg', BOT, 'POST', {'message_id': old['id']})
                assert reply['retcode'] == 0 and reply['data'] == old['data'], reply
            crash_recovery = None
            receipt_recovery = None
            first_peer = None
            if loss:
                before_drivers={name:inspect(name) for name in ACCOUNTS}
                command_at=time.monotonic()
                request_json(points['control']+'/fixture/inject',CONTROL,'POST',{'account':ACCOUNTS[0],'kind':'private','text':'/receipt-send','mentioned':False})
                wait_for(lambda:relay.held.is_set() and json.loads(peer_report.read_text()).get('receipt_pending'),processes)
                held=relay.snapshot()['held_reply'];accepted=held['response']['data']
                interrupted_peer=json.loads(peer_report.read_text());record=interrupted_peer['receipt_attempt']['record']
                assert accepted['wechat_request_id']==record['request_id'] and not held['forwarded']
                assert not interrupted_peer.get('receipt_results') and not interrupted_peer.get('receipt_error')
                wait_for(lambda:len(inspect(ACCOUNTS[0])['sends'])==len(before_drivers[ACCOUNTS[0]]['sends'])+1,processes)
                committed=inspect(ACCOUNTS[0])['sends'][-1]
                assert committed['id']==accepted['wechat_delivery_id'] and committed['payload_sha256']==record['sha256']
                target=admin('/accounts/'+ACCOUNTS[0]+'/history')['cursor']
                wait_for(lambda:next(row for row in accounts() if row['name']==ACCOUNTS[0])['connections'][0].get('cursor',0)>=target,processes)
                row_a=next(row for row in accounts() if row['name']==ACCOUNTS[0])
                delivery_before=request_json('http://'+row_a['onebot_address']+'/wechat_get_delivery',BOT,'POST',{'delivery_id':accepted['wechat_delivery_id']})
                assert delivery_before['retcode']==0 and delivery_before['data']['state']=='accepted'
                archive_before=admin('/accounts/'+ACCOUNTS[0]+'/archive')
                active_before=admin('/accounts/'+ACCOUNTS[0]+'/deliveries')
                assert any(job['id']==accepted['wechat_delivery_id'] for job in active_before)
                interrupted_peer=json.loads(peer_report.read_text())
                assert interrupted_peer['receipt_pending'] and not interrupted_peer.get('receipt_results') and not interrupted_peer.get('receipt_error')
                if runtime_crash:
                    api_timeout=interrupted_peer['api_timeout_seconds']
                    assert api_timeout==(180 if application else 30),api_timeout
                kill_age=time.monotonic()-command_at
                assert kill_age<10,'framework API may no longer be pending before the deliberate crash'
                kept_peer_pid=peer.pid
                killed_host_pid=host_process.pid if runtime_crash else None
                if runtime_crash:
                    # Keep the actual consumer alive and kill before archival
                    # or orderly close: only persisted native/ID state may win.
                    kill_owned(host_process)
                    wait_for(lambda:not relay.snapshot()['ready_ids'],processes)
                    assert peer.poll() is None and peer.pid==kept_peer_pid
                    compact=None
                    archive_after=archive_before
                else:
                    kill_owned(peer)
                    wait_for(lambda:not relay.snapshot()['ready_ids'] and all(row['connections'][0]['reconnects']>=1 for row in accounts()),processes)
                    compact=admin('/accounts/'+ACCOUNTS[0]+'/archive/compact','POST',{})
                    archive_after=admin('/accounts/'+ACCOUNTS[0]+'/archive')
                    assert archive_after['archive']['archived_jobs']>archive_before['archive']['archived_jobs']
                    assert all(job['id']!=accepted['wechat_delivery_id'] for job in admin('/accounts/'+ACCOUNTS[0]+'/deliveries'))
                    stop(host_process)
                host_process,host_log=start('host')
                points=wait_for(addresses,processes)
                assert request_json(points['control']+'/fixture/identity',CONTROL)['source_manifest_sha256']==source_sha
                wait_for(lambda:len(accounts())==2,processes)
                reopened={row['name']:row for row in accounts()}
                cold={name:inspect(name) for name in ACCOUNTS}
                assert all(not row['sends'] and row['synthetic_uploads']==0 for row in cold.values()),cold
                base_a='http://'+reopened[ACCOUNTS[0]]['onebot_address']
                base_b='http://'+reopened[ACCOUNTS[1]]['onebot_address']
                delivery_after=request_json(base_a+'/wechat_get_delivery',BOT,'POST',{'delivery_id':accepted['wechat_delivery_id']})
                assert delivery_after==delivery_before
                lookup_after=request_json(base_a+'/wechat_get_delivery_by_request',BOT,'POST',{'wechat_request_id':record['request_id']})
                assert lookup_after['retcode']==0 and lookup_after['data']['receipt']==accepted
                retained=request_json(base_a+'/get_msg',BOT,'POST',{'message_id':accepted['message_id']})
                assert retained['retcode']==0
                part=next(part for part in retained['data']['message'] if part['type']=='file')
                assert hashlib.sha256(asyncio.run(fetch(part['data']['url']))).hexdigest()==record['sha256']
                assert request_json(base_b+'/wechat_get_delivery',BOT,'POST',{'delivery_id':accepted['wechat_delivery_id']})['retcode']!=0
                assert request_json(base_b+'/wechat_get_delivery_by_request',BOT,'POST',{'wechat_request_id':record['request_id']})['retcode']!=0
                if runtime_crash:
                    assert host_process.pid!=killed_host_pid and peer.poll() is None and peer.pid==kept_peer_pid
                else:
                    peer_report.unlink();peer,_=start('peer')
                wait_for(listening,processes);wait_for(connected,processes)
                assert len(relay.snapshot()['requests'])==1,'request was retried without consumer instruction'
                request_json(points['control']+'/fixture/inject',CONTROL,'POST',{'account':ACCOUNTS[0],'kind':'private','text':'/receipt-retry','mentioned':False})
                wait_for(lambda:json.loads(peer_report.read_text()).get('receipt_results'),processes)
                retried=json.loads(peer_report.read_text())['receipt_results'][0]
                assert retried['record']==record and retried['result']==accepted,retried
                consumer_lookup=json.loads(peer_report.read_text())['receipt_lookup']
                assert consumer_lookup['receipt']==accepted and consumer_lookup['delivery']['state']=='accepted'
                assert all(inspect(name)==cold[name] for name in ACCOUNTS),'archived duplicate re-executed native work'
                assert admin('/accounts/'+ACCOUNTS[0]+'/archive')['archive']['native_reservations']==archive_after['archive']['native_reservations']
                conflict=request_json(base_a+'/send_private_msg',BOT,'POST',{'user_id':record['user_id'],'message':'different','wechat_request_id':record['request_id']})
                assert conflict['retcode']!=0 and conflict['message']=='idempotency_conflict'
                sibling_body=payload('file',ACCOUNTS[1])
                sibling_record={**record,'self_id':initial[per_account]['self_id'],'user_id':initial[per_account]['user_id'],'label':ACCOUNTS[1],
                    'bytes':len(sibling_body),'sha256':hashlib.sha256(sibling_body).hexdigest()}
                sibling=request_json(base_b+'/send_private_msg',BOT,'POST',receipt_parameters(sibling_record))
                assert sibling['retcode']==0 and sibling['data']['message_id']!=accepted['message_id'] and sibling['data']['wechat_delivery_id']!=accepted['wechat_delivery_id']
                wait_for(lambda:len(inspect(ACCOUNTS[1])['sends'])==1,processes)
                assert not inspect(ACCOUNTS[0])['sends'] and inspect(ACCOUNTS[0])['synthetic_uploads']==0
                sibling_lookup=request_json(base_b+'/wechat_get_delivery_by_request',BOT,'POST',{'wechat_request_id':record['request_id']})
                original_lookup=request_json(base_a+'/wechat_get_delivery_by_request',BOT,'POST',{'wechat_request_id':record['request_id']})
                assert sibling_lookup['data']['receipt']==sibling['data'] and original_lookup['data']['receipt']==accepted
                receipt_recovery={'interrupted_stage':'accepted native receipt/public IDs persisted; OneBot response withheld before consumer',
                    'framework_exit':None if runtime_crash else -signal.SIGKILL,'runtime_exit':-signal.SIGKILL if runtime_crash else None,'command_to_kill_millis':int(kill_age*1000),'held_reply':held,'consumer_record':record,'interrupted_peer_report':interrupted_peer,
                    'original_native_commit':committed,'archive_before':archive_before,'compact_result':compact,'archive_after':archive_after,
                    'host_reopened_before_explicit_retry':True,'active_job_removed_before_reopen':not runtime_crash,'active_jobs_before_crash':active_before,'framework_process_retained':runtime_crash,'retained_framework_pid':kept_peer_pid if runtime_crash else None,'killed_runtime_pid':killed_host_pid,'delivery_before':delivery_before,'delivery_after':delivery_after,
                    'retained_file_sha256':record['sha256'],'reopened_driver_facts_before_retry':cold,'consumer_lookup':consumer_lookup,'lookup_after_reopen':lookup_after,'consumer_retry_result':retried,'changed_content_conflict':conflict,
                    'sibling_same_key_new_receipt':sibling,'sibling_same_key_lookup':sibling_lookup,'original_lookup_after_sibling':original_lookup,'automatic_request_retries':0,'native_duplicate_submissions':0}
            elif crash:
                # A real action reads an incomplete source before Submit.
                # Killing the framework must cancel that read, not enqueue or
                # retry a send. Native CDN/receipt ambiguity is a separate case.
                before_rows = {row['name']: row for row in accounts()}
                before_drivers = {name: inspect(name) for name in ACCOUNTS}
                def deliveries():
                    return {name: sorted(admin('/accounts/'+name+'/deliveries'),key=lambda row:row['id']) for name in ACCOUNTS}
                before_jobs = deliveries()
                command_at = time.monotonic()
                request_json(points['control']+'/fixture/inject',CONTROL,'POST',{'account':ACCOUNTS[0],'kind':'private','text':'/crash-transfer','mentioned':False})
                wait_for(lambda: stalled.entered.is_set() and peer_report.exists() and 'crash_attempt' in json.loads(peer_report.read_text()),processes)
                target = admin('/accounts/'+ACCOUNTS[0]+'/history')['cursor']
                wait_for(lambda:next(row for row in accounts() if row['name']==ACCOUNTS[0])['connections'][0].get('cursor',0)>=target,processes)
                assert len(stalled.requests)==1 and stalled.requests[0]['user_agent']=='Go-http-client/1.1',stalled.requests
                assert not stalled.closed.is_set(), 'source read ended before the deliberate crash'
                interrupted_peer = json.loads(peer_report.read_text())
                attempted = interrupted_peer['crash_attempt']
                assert (attempted['self_id'],attempted['user_id'])==(initial[0]['self_id'],initial[0]['user_id'])
                kill_at = time.monotonic()
                timing = cancellation_window(command_at,stalled.entered_at,kill_at)
                peer.kill()
                peer.wait(timeout=5)
                assert peer.returncode == -signal.SIGKILL,peer.returncode
                processes.remove(peer)
                assert stalled.closed.wait(timeout=2),'framework crash left the runtime source request blocked'
                cancel_millis = int((stalled.closed_at-kill_at)*1000)
                assert 0 <= cancel_millis < 2000,cancel_millis
                disconnected = wait_for(lambda:accounts() if all(row['connections'][0]['state']=='reconnecting' and row['connections'][0]['reconnects']>=1 for row in accounts()) else None,processes)
                assert deliveries()==before_jobs,'incomplete source created or changed a native delivery'
                for row in disconnected:
                    name=row['name'];assert row['onebot_address']==before_rows[name]['onebot_address']
                    assert inspect(name)==before_drivers[name],'crash changed native owner or send/download/upload facts'
                    status=request_json('http://'+row['onebot_address']+'/get_status',BOT)
                    assert status['retcode']==0 and status['data']['good']
                old_message=request_json('http://'+before_rows[ACCOUNTS[0]]['onebot_address']+'/get_msg',BOT,'POST',{'message_id':attempted['received_message_id']})
                assert old_message['retcode']==0 and old_message['data']['raw_message']=='/crash-transfer'
                peer_report.unlink()
                peer,_=start('peer')
                wait_for(listening,processes)
                # Keep the saved/applied connection plan: recovery must use
                # ordinary reverse reconnect, without a management apply.
                recovered=wait_for(connected,processes)
                assert all(row['connections'][0]['reconnects']>=1 for row in recovered)
                assert all(row['onebot_address']==before_rows[row['name']]['onebot_address'] for row in recovered)
                assert deliveries()==before_jobs
                crash_recovery={'signal':'SIGKILL','exit_code':-signal.SIGKILL,'interrupted_stage':'runtime outbound source fetch before Submit','source_declared_bytes':stalled.declared_bytes,'source_sent_bytes':1,'source_requests':list(stalled.requests),'source_cancel_millis':cancel_millis,
                    'cancellation_causality_window':timing,
                    'native_deliveries_unchanged_before_reconnect':True,'native_owner_generation_and_ports_retained':True,'management_and_onebot_remained_responsive':True,'automatic_reverse_reconnect':True,'automatic_source_retries':0,'transport_cursor_committed_before_crash':target,'interrupted_message_still_queryable':True,
                    'interrupted_peer_report':interrupted_peer,'reconnecting_accounts':disconnected,'recovered_accounts':recovered}
            else:
                # Graceful variant closes managed connections before SIGTERM.
                change_connections(False)
                first_peer = stop(peer, peer_kind=framework)
                peer_report.unlink()
                peer, _ = start('peer')
                wait_for(listening, processes)
                change_connections(True)
                wait_for(connected, processes)
            after_peer = exercise('runtime_crash_reopen' if runtime_crash else 'receipt_loss_recovery' if loss else 'framework_crash_reopen' if crash else 'framework_reopen', per_round*2 if runtime_crash else 0)
            if crash:
                assert 'crash_attempt' not in json.loads(peer_report.read_text()),'transport cursor replayed the interrupted command'
                assert len(stalled.requests)==1,'interrupted source was automatically retried'
            for first, after in zip(initial, after_peer):
                assert (first['self_id'], first['user_id']) == (after['self_id'], after['user_id'])
            # Remove/re-add one production account owner while retaining its
            # sibling, using the ordinary authenticated topology interface.
            before = {row['name']: row for row in accounts()}
            sibling_generation = inspect('synthetic-b')['generation']
            sibling_downloads = inspect('synthetic-b')['synthetic_downloads']
            old_generation = inspect('synthetic-a')['generation']
            current = admin('/topology')
            admin('/topology', 'POST', {'revision': current['revision'], 'plan': {'version': 1, 'accounts': [row for row in topology['accounts'] if row['name'] != 'synthetic-a']}})
            admin('/topology/apply', 'POST', {})
            remaining = wait_for(lambda: connected(['synthetic-b']), processes)
            assert remaining[0]['onebot_address'] == before['synthetic-b']['onebot_address']
            assert inspect('synthetic-b')['generation'] == sibling_generation
            old_port = int(before['synthetic-a']['onebot_address'].rsplit(':', 1)[1])
            with socket.socket() as check:
                check.settimeout(1)
                assert check.connect_ex(('127.0.0.1', old_port)) != 0
            current = admin('/topology')
            admin('/topology', 'POST', {'revision': current['revision'], 'plan': topology})
            admin('/topology/apply', 'POST', {})
            wait_for(connected, processes)
            assert inspect('synthetic-a')['generation'] != old_generation
            assert inspect('synthetic-b')['generation'] == sibling_generation
            account_reenable_downloads={name:inspect(name)['synthetic_downloads'] for name in ACCOUNTS}
            assert account_reenable_downloads=={'synthetic-a':0,'synthetic-b':sibling_downloads},account_reenable_downloads
            media_references_still_readable(old_media,account_reenable_downloads)
            after_membership = exercise('account_reenable', per_round*3 if runtime_crash else per_round)
            for first, after in zip(initial, after_membership):
                assert (first['self_id'], first['user_id']) == (after['self_id'], after['user_id'])
            final_incoming_cache={name:admin('/accounts/'+name+'/media-cache') for name in ACCOUNTS}
            assert all(row['active_native_tasks']==0 for row in final_incoming_cache.values())
            if runtime_crash:
                # The retained consumer's original API future must reach its
                # own bounded failure. A new successful retry cannot stand in
                # for that task, and shutdown must not hide a pending future.
                quiet={name:inspect(name) for name in ACCOUNTS}
                def finished_original():
                    return original_completion(json.loads(peer_report.read_text()).get('receipt_completions',[]),command_at,api_timeout)
                completed=finished_original()
                if completed is None:
                    remaining=api_timeout+15-(time.monotonic()-command_at)
                    assert remaining>0,'consumer exceeded the original API completion deadline'
                    completed=wait_for(finished_original,processes,timeout=remaining)
                assert completed['outcome']=='error',completed
                assert all(inspect(name)==quiet[name] for name in ACCOUNTS),'failed original API call caused more native work'
                receipt_recovery['original_send_completion']=completed
                receipt_recovery['original_api_wait_completed_before_shutdown']=True
                receipt_recovery['original_api_wait']={'configured_timeout_seconds':api_timeout,'command_to_completion_millis':int((completed['completed_monotonic']-command_at)*1000),'command_to_observation_millis':int((time.monotonic()-command_at)*1000),'command_deadline_seconds':api_timeout+15,'timeout_observation':'Read only actual client configuration; client future/timeout unchanged'}
            change_connections(False)
            final_peer = stop(peer, peer_kind=framework)
            stop(host_process)
            assert not processes, 'successful deployment left children for fallback cleanup'
            if stalled is not None:
                assert len(stalled.requests)==1 and stalled.closed.is_set()
                stalled.close()
                stalled=None
                crash_recovery['source_server_joined']=True
            if relay is not None:
                snapshot=relay.snapshot();assert not snapshot['failures'],snapshot
                assert [row['action'] for row in snapshot['requests']]==['send_private_msg','wechat_get_delivery_by_request','send_private_msg'],snapshot
                assert snapshot['forwarded_confirmed_replies']==1 and not snapshot['ready_ids'],snapshot
                relay.close();relay=None
                receipt_recovery['relay_final']=snapshot;receipt_recovery['relay_joined']=True
            assert hashlib.sha256(binary.read_bytes()).hexdigest() == binary_sha
            assert load_checked()[1] == source_sha
            result = {'result': 'pass', 'framework': framework, 'framework_versions': json.loads(peer_report.read_text())['versions'],
                'scope': 'Separate actual Host/Service/OneBot/storage/management process and actual framework process; synthetic native ingestion and terminal receipts; no native protocol session loop',
                'framework_application_lifecycle': framework == 'nonebot' or application, 'astrbot_full_application_started': application,
                'observations': observations, 'listener_swap':listener_swap_evidence, 'old_messages_stable_after_host_reopen': True, 'interrupted_http_reads':interrupted_http_reads, 'final_incoming_cache':final_incoming_cache,
                'old_media_references_cached_after_host_and_account_reopen':bool(old_media),
                'downloads_before_new_injection_after_host_reopen':host_reopen_downloads,
                'downloads_before_account_stop':{'synthetic-b':sibling_downloads},
                'downloads_before_new_injection_after_account_reenable':account_reenable_downloads,
                'cross_account_signed_media_denied':bool(old_media),
                'account_stop_reenable_preserved_ids': True, 'retained_sibling_worker_and_port': True,
                'host_process_starts': starts['host'], 'framework_process_starts': starts['peer'],
                'synthetic_replies':per_round*4, 'scenario':scenario, 'large_media_payload_bytes':CORPUS['large_payload_bytes'] if large else 0, 'synthetic_media_replies':(per_round-4)*4, 'graceful_host_shutdowns': starts['host']-int(runtime_crash),
                'nonebot_shutdown_hooks': ([final_peer] if crash or loss else [first_peer, final_peer]) if framework == 'nonebot' else [],
                'astrbot_adapter_shutdowns': ([final_peer] if crash or loss else [first_peer,final_peer]) if framework in ('astrbot','astrbot-application') else [],
                'astrbot_application_shutdowns':([final_peer] if crash or loss else [first_peer,final_peer]) if application else [],
                'persistent_application_user_counters_verified':application,
                'no_forced_kill_in_success_path': not (crash or loss),'deliberate_framework_crash':crash_recovery,'receipt_loss_recovery':receipt_recovery,'runtime_crash_recovery':receipt_recovery if runtime_crash else None, 'no_cleanup_kill_in_success_path':True, 'real_protocol_calls': 0, 'live_wechat_or_model_requests': 0,
                'runtime_fixture_binary_sha256': binary_sha,
                'binary_snapshot':'private executable copy verified before each start and after all host runs',
                'peer_snapshot':'private read-only peer/probe scripts and pinned committed AstrBot archive',
                'deployment_source_path':'.bin/reports/deployment-source-'+source_sha+'.json',
                'deployment_source_sha256':source_sha,
                'limitations': ['Synthetic terminalization does not execute the native text transport or prove live delivery',
                    'Actual Host SIGKILL after known synthetic acceptance; consumer stays alive; not unknown native platform commit or power loss' if runtime_crash else 'OneBot response is lost after a known synthetic accepted receipt; not native platform receipt loss' if loss else 'Deliberate crash interrupts source fetch before Submit, not native CDN, post-commit receipt ambiguity or runtime process crash' if crash else 'Framework reopen is graceful in this scenario',
                    'No exactly-once consumer processing guarantee',
                    'AstrBot application uses actual InitialLoader/Core/plugin/pipeline/SQLite with providers and dashboard listener disabled; metadata endpoint points to owned loopback' if application else 'AstrBot uses its real adapter and local consumer; no full application in this scenario',
                    'CDN/profile discovery and platform exchanges are synthetic; native media workers/preparation/body encoders are exercised locally']}
            output.write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps({'result': 'pass', 'framework': framework, 'synthetic_replies':per_round*4, 'scenario':scenario, 'large_media_payload_bytes':CORPUS['large_payload_bytes'] if large else 0, 'synthetic_media_replies':(per_round-4)*4, 'host_starts':starts['host'],'framework_starts':starts['peer']}))
        except Exception:
            for log in logs:
                log.flush(); log.seek(0); sys.stderr.write(log.read()[-10000:])
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in reversed(processes):
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=3)
            for log in logs:
                log.close()
            if stalled is not None:
                stalled.close()
            if relay is not None:
                relay.close()


if __name__ == '__main__':
    run(sys.argv[1] if len(sys.argv) > 1 else 'nonebot',sys.argv[2] if len(sys.argv)>2 else 'text')
