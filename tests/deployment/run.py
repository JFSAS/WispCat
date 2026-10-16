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
from media_probe import payload,fetch,local_media_url

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
    assert framework in ('nonebot', 'astrbot','astrbot-application') and scenario in ('text','media')
    application=framework=='astrbot-application'
    media_kinds=['text','image','file'] if scenario=='media' else ['text']
    cases=[(kind,media) for media in media_kinds for kind in ['private','group']]
    per_account=len(cases)
    per_round=per_account*len(ACCOUNTS)
    source, source_sha = load_checked()
    output = ROOT/('.bin/deployment-application-results' if application else '.bin/deployment-media-results' if scenario=='media' else '.bin/deployment-results')/(framework+'.json')
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
            'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUNBUFFERED': '1',
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

        try:
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
                return rows if sorted(row['name'] for row in rows) == sorted(names) and all(len(row['connections']) == 1 and row['connections'][0]['state'] == 'connected' for row in rows) else None

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
                        if media!='text':value['media']=media
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
            after_host = exercise('host_reopen', per_round)
            for first, after in zip(initial, after_host):
                assert (first['self_id'], first['user_id']) == (after['self_id'], after['user_id'])
            for old in old_messages:
                row = next(row for row in restarted_accounts if row['name'] == old['account'])
                reply = request_json('http://' + row['onebot_address'] + '/get_msg', BOT, 'POST', {'message_id': old['id']})
                assert reply['retcode'] == 0 and reply['data'] == old['data'], reply
            # Close managed connections before stopping the actual framework so
            # a graceful lifecycle is tested, with no SIGKILL success fallback.
            change_connections(False)
            first_peer = stop(peer, peer_kind=framework)
            peer_report.unlink()
            peer, _ = start('peer')
            wait_for(listening, processes)
            change_connections(True)
            wait_for(connected, processes)
            after_peer = exercise('framework_reopen', 0)
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
            after_membership = exercise('account_reenable', per_round)
            for first, after in zip(initial, after_membership):
                assert (first['self_id'], first['user_id']) == (after['self_id'], after['user_id'])
            change_connections(False)
            final_peer = stop(peer, peer_kind=framework)
            stop(host_process)
            assert hashlib.sha256(binary.read_bytes()).hexdigest() == binary_sha
            assert load_checked()[1] == source_sha
            result = {'result': 'pass', 'framework': framework, 'framework_versions': json.loads(peer_report.read_text())['versions'],
                'scope': 'Separate actual Host/Service/OneBot/storage/management process and actual framework process; synthetic native ingestion and terminal receipts; no native protocol session loop',
                'framework_application_lifecycle': framework == 'nonebot' or application, 'astrbot_full_application_started': application,
                'observations': observations, 'old_messages_stable_after_host_reopen': True,
                'old_media_references_cached_after_host_and_account_reopen':bool(old_media),
                'downloads_before_new_injection_after_host_reopen':host_reopen_downloads,
                'downloads_before_account_stop':{'synthetic-b':sibling_downloads},
                'downloads_before_new_injection_after_account_reenable':account_reenable_downloads,
                'cross_account_signed_media_denied':bool(old_media),
                'account_stop_reenable_preserved_ids': True, 'retained_sibling_worker_and_port': True,
                'host_process_starts': starts['host'], 'framework_process_starts': starts['peer'],
                'synthetic_replies':per_round*4, 'scenario':scenario, 'synthetic_media_replies':(per_round-4)*4, 'graceful_host_shutdowns': 2,
                'nonebot_shutdown_hooks': [first_peer, final_peer] if framework == 'nonebot' else [],
                'astrbot_adapter_shutdowns': [first_peer,final_peer] if framework in ('astrbot','astrbot-application') else [],
                'astrbot_application_shutdowns':[first_peer,final_peer] if application else [],
                'persistent_application_user_counters_verified':application,
                'no_forced_kill_in_success_path': True, 'real_protocol_calls': 0, 'live_wechat_or_model_requests': 0,
                'runtime_fixture_binary_sha256': binary_sha,
                'binary_snapshot':'private executable copy verified before each start and after both host runs',
                'peer_snapshot':'private read-only peer/probe scripts and pinned committed AstrBot archive',
                'deployment_source_path':'reports/deployment-source-'+source_sha+'.json',
                'deployment_source_sha256':source_sha,
                'limitations': ['Synthetic terminalization does not execute the native text transport or prove live delivery',
                    'No exactly-once consumer processing guarantee',
                    'AstrBot application uses actual InitialLoader/Core/plugin/pipeline/SQLite with providers and dashboard listener disabled; metadata endpoint points to owned loopback' if application else 'AstrBot uses its real adapter and local consumer; no full application in this scenario',
                    'CDN/profile discovery and platform exchanges are synthetic; native media workers/preparation/body encoders are exercised locally']}
            output.write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps({'result': 'pass', 'framework': framework, 'synthetic_replies':per_round*4, 'scenario':scenario, 'synthetic_media_replies':(per_round-4)*4, 'host_starts': 2, 'framework_starts': 2}))
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


if __name__ == '__main__':
    run(sys.argv[1] if len(sys.argv) > 1 else 'nonebot',sys.argv[2] if len(sys.argv)>2 else 'text')
