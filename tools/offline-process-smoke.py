"""Start the production binary with empty private state and two offline accounts.

Run inside the same loopback-only sandbox as the Go tests. Never requests login,
account import or online mode. No existing state or global configuration is read.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import tempfile
import time
from offline_api import probe_offline_accounts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    with tempfile.TemporaryDirectory(prefix='wechat-offline-process-') as directory:
        temp = Path(directory)
        config = temp / 'runtime.json'
        config.write_text(json.dumps({
            'state_dir': str(temp / 'state'), 'management_listen': '127.0.0.1:0',
            'management_token_env': 'FIXTURE_ADMIN_TOKEN', 'accounts': [
                {'name': name, 'instance': 'instance:' + name, 'mode': 'offline',
                 'listen': '127.0.0.1:0', 'token_env': 'FIXTURE_BOT_TOKEN'}
                for name in ['first', 'second']]}))
        env = {'HOME': str(temp), 'PATH': os.environ['PATH'],
               'FIXTURE_ADMIN_TOKEN': 'synthetic-admin-token',
               'FIXTURE_BOT_TOKEN': 'synthetic-onebot-token'}
        if 'LD_LIBRARY_PATH' in os.environ:
            env['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH']
        log = temp / 'runtime.log'
        process = None
        with log.open('w+') as output:
            try:
                process = subprocess.Popen([str(binary), 'serve', '--config', str(config)],
                    cwd=temp, env=env, stdout=output, stderr=subprocess.STDOUT)
                base = None
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError('runtime exited during startup: ' + log.read_text())
                    base = next((line.removeprefix('management: ').strip()
                        for line in log.read_text().splitlines() if line.startswith('management: ')), None)
                    if base:
                        break
                    time.sleep(.05)
                assert base, 'management listener did not start'

                summaries = probe_offline_accounts(base, env['FIXTURE_ADMIN_TOKEN'], env['FIXTURE_BOT_TOKEN'])
                process.send_signal(signal.SIGTERM)
                assert process.wait(timeout=10) == 0, log.read_text()
                result = {'result': 'pass', 'system': platform.system(), 'machine': platform.machine(),
                    'fixture': 'production binary, empty synthetic state', 'accounts': summaries,
                    'graceful_shutdown': True, 'live_wechat_or_model_requests': 0}
                args.report.write_text(json.dumps(result, indent=2) + '\n')
                print(json.dumps(result))
            finally:
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)


if __name__ == '__main__':
    main()
