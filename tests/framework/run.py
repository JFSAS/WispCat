"""Run a real framework against the Go runtime and a synthetic channel.

No WeChat protocol worker is created. Run under the offline network sandbox.
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
TOKEN = 'synthetic-framework-token'
ACCOUNTS = ['synthetic-account-a', 'synthetic-account-b']

def unused_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def request(base, path, value=None):
    raw = None if value is None else json.dumps(value).encode()
    q = urllib.request.Request(base + path, data=raw,
        headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(q, timeout=2) as response:
        return json.load(response)

def wait_for(callback, processes, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f'fixture process exited: {process.returncode}')
        try:
            result = callback()
            if result:
                return result
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.05)
    raise TimeoutError('local framework fixture did not reach its expected state')

def run(framework='nonebot'):
    with tempfile.TemporaryDirectory(prefix='wechat-framework-') as directory:
        temp = Path(directory)
        port = unused_port()
        report = temp / 'framework.json'
        env = {'PATH': os.environ['PATH'], 'HOME': str(temp), 'PYTHONNOUSERSITE': '1',
               'PYTHONUNBUFFERED': '1', 'FRAMEWORK_PORT': str(port), 'FRAMEWORK_REPORT': str(report),
               'ASTRBOT_ROOT': str(temp), 'ASTRBOT_DISABLE_METRICS': '1'}
        processes = []
        logs = []
        try:
            peer_log = (temp / 'peer.log').open('w+')
            logs.append(peer_log)
            peer = subprocess.Popen([sys.executable, str(ROOT / 'tests/framework' / f'{framework}_peer.py')],
                                    cwd=temp, env=env, stdout=peer_log, stderr=subprocess.STDOUT)
            processes.append(peer)
            def listening():
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    return True
            wait_for(listening, processes)
            fixture_log = (temp / 'fixture.log').open('w+')
            logs.append(fixture_log)
            path = '/onebot/v11/ws' if framework == 'nonebot' else '/ws'
            fixture = subprocess.Popen([str(ROOT / '.bin/framework-fixture'), '--reverse',
                f'ws://127.0.0.1:{port}{path}', '--state-dir', str(temp / 'state')],
                cwd=temp, env=env, stdout=fixture_log, stderr=subprocess.STDOUT)
            processes.append(fixture)
            def control_address():
                return next((line.split('control: ', 1)[1].strip() for line in
                             (temp / 'fixture.log').read_text().splitlines() if line.startswith('control: ')), None)
            base = wait_for(control_address, processes)
            def connections_at(state, cursor=0):
                connections = request(base, '/status')['connections']
                return len(connections) == len(ACCOUNTS) and all(
                    c['state'] == state and c.get('cursor', 0) >= cursor for c in connections)
            def exercise():
                result = None
                count = 0
                for account in ACCOUNTS:
                    for kind in ['private', 'group']:
                        request(base, '/inject', {'account': account, 'kind': kind,
                            'text': f'/{kind}-ping', 'mentioned': kind == 'group'})
                        count += 1
                        result = wait_for(lambda: json.loads(report.read_text()) if report.exists()
                            and len(json.loads(report.read_text())['handled']) == count else None, processes)
                return result
            def validate(result, rounds):
                sends = request(base, '/sends')
                assert len(sends) == rounds * 4, sends
                for account in ACCOUNTS:
                    scoped = [q for q in sends if q['Account'] == account]
                    assert len(scoped) == rounds * 2, sends
                    for private, group in zip(scoped[::2], scoped[1::2]):
                        assert private['Conversation'] == 'friend' and private['Text'] == 'pong:/private-ping', sends
                        assert group['Conversation'] == 'room' and group['Text'] == ' pong:/group-ping', sends
                        assert group['Mentions'] == ['friend'], sends
                handled = result['handled']
                assert len(handled) == 4, handled
                for private, group in zip(handled[::2], handled[1::2]):
                    assert private['self_id'] == group['self_id'], handled
                    assert private['user_id'] == group['user_id'], handled
                    assert group['to_me'] and group['member_lookup_verified'], handled
                assert handled[0]['self_id'] != handled[2]['self_id'], handled
                assert handled[0]['user_id'] != handled[2]['user_id'], handled
                return sends
            wait_for(lambda: connections_at('connected'), processes)
            result = exercise()
            validate(result, 1)
            initial = result['handled']
            wait_for(lambda: connections_at('connected', 2), processes)
            peer.send_signal(signal.SIGTERM)
            try:
                peer.wait(timeout=5)
            except subprocess.TimeoutExpired:
                peer.kill()
                peer.wait(timeout=2)
            processes.remove(peer)
            wait_for(lambda: connections_at('reconnecting'), processes)
            report.unlink()
            restart_log = (temp / 'peer-restart.log').open('w+')
            logs.append(restart_log)
            peer = subprocess.Popen([sys.executable, str(ROOT / 'tests/framework' / f'{framework}_peer.py')],
                                    cwd=temp, env=env, stdout=restart_log, stderr=subprocess.STDOUT)
            processes.append(peer)
            wait_for(listening, processes)
            wait_for(lambda: connections_at('connected', 2), processes)
            resumed = exercise()
            sends = validate(resumed, 2)
            for before, after in zip(initial, resumed['handled']):
                assert after['self_id'] == before['self_id'], resumed
                assert after['user_id'] == before['user_id'], resumed
            result.update(result='pass', synthetic_channel=True, production_protocol_worker=False,
                          live_wechat_requests=0, sent_replies=len(sends), reconnect_verified=True,
                          reconnect_handled=resumed['handled'], multi_account_verified=True,
                          account_count=len(ACCOUNTS))
            target = ROOT / 'reports' / f'{framework}-integration.json'
            target.write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result))
        except Exception:
            for log in logs:
                log.flush()
                log.seek(0)
                sys.stderr.write(log.read()[-12000:])
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in reversed(processes):
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            for log in logs:
                log.close()

if __name__ == '__main__':
    run(sys.argv[1] if len(sys.argv) > 1 else 'nonebot')
