"""Exercise a locally built production image with network disabled throughout.

Image pulls and dependency preparation must finish before this command. Only
task-owned containers/volumes are created and removed; no host ports are exposed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parent.parent
LABEL = 'org.wispcat.offline-fixture'


def binary_build_info(binary):
    env = {**os.environ, 'GOTOOLCHAIN': 'local', 'GOPROXY': 'off',
           'GOSUMDB': 'off', 'GOENV': 'off', 'GOTELEMETRY': 'off'}
    return subprocess.check_output(['go', 'version', '-m', str(binary)],
                                   text=True, env=env, timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='local Docker context with a Unix socket')
    parser.add_argument('--image', required=True, help='already built runtime image; never pulled')
    parser.add_argument('--probe-image', default='python:3.13-alpine', help='already available Python image')
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    docker = ['docker', '--context', args.context]

    def run(*words, check=True, timeout=60):
        result = subprocess.run(docker + list(words), text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError('docker command failed: ' + ' '.join(words[:2]) + ': ' + result.stderr.strip()[-1600:])
        return result

    def inspect_optional(kind, name):
        words = ('volume', 'inspect', name) if kind == 'volume' else ('inspect', name)
        found = run(*words, check=False)
        if found.returncode == 0:
            return json.loads(found.stdout)[0]
        missing = ('no such volume' in found.stderr.lower() if kind == 'volume'
                   else 'no such object' in found.stderr.lower() or 'no such container' in found.stderr.lower())
        if not missing:
            raise RuntimeError('Cannot confirm fixture resource state: ' + found.stderr[:300])
        return None

    endpoint = json.loads(run('context', 'inspect', args.context).stdout)[0]['Endpoints']['docker']['Host']
    if not endpoint.startswith('unix://'):
        raise SystemExit('Container checks require a local Unix-socket Docker context')
    image = json.loads(run('image', 'inspect', args.image).stdout)[0]
    probe_image = json.loads(run('image', 'inspect', args.probe_image).stdout)[0]
    assert image['Os'] == probe_image['Os'] == 'linux'
    assert image['Config']['User'] not in ['', '0', 'root'], 'runtime image must declare a nonroot user'
    input_paths = [ROOT / 'Dockerfile', ROOT / 'tools/container-offline-checks.py', ROOT / 'tools/container-probe.py', ROOT / 'tools/offline_api.py']
    input_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in input_paths}
    engine = json.loads(run('version', '--format', '{{json .Server}}').stdout)
    task = uuid.uuid4().hex
    prefix = 'wispcat-offline-' + task
    volume, primary, contender, probe = [prefix + suffix for suffix in ['-state', '-runtime', '-contender', '-probe']]
    containers = [primary, contender, probe]
    labels = ['--label', LABEL + '=' + task]
    started = time.monotonic()
    results = []
    stopped = []
    cleanup = []
    runtime_details = None
    binary_metadata = None
    libraries = None
    package_versions = None
    result_report = None
    (ROOT / '.bin').mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='container-fixture-', dir=ROOT / '.bin') as raw:
        directory = Path(raw)
        config = directory / 'runtime.json'
        config.write_text(json.dumps({'state_dir': '/home/runtime/state',
            'management_listen': '127.0.0.1:5099', 'management_token_env': 'FIXTURE_ADMIN_TOKEN',
            'accounts': [{'name': name, 'instance': 'instance:' + name, 'mode': 'offline',
                          'listen': '127.0.0.1:0', 'token_env': 'FIXTURE_BOT_TOKEN'}
                         for name in ['first', 'second']]}))
        config.chmod(0o644)  # Contains environment names only, no credentials.
        common = ['--network', 'none', '--read-only', '--cap-drop', 'ALL',
                  '--security-opt', 'no-new-privileges', '--memory', '256m',
                  '--tmpfs', '/tmp:rw,noexec,nosuid,size=16m,mode=1777',
                  '--mount', 'type=volume,src=' + volume + ',dst=/home/runtime',
                  '--mount', 'type=bind,src=' + str(config) + ',dst=/config/runtime.json,readonly',
                  '--env', 'FIXTURE_ADMIN_TOKEN=synthetic-admin-token',
                  '--env', 'FIXTURE_BOT_TOKEN=synthetic-onebot-token',
                  '--env', 'HTTP_PROXY=', '--env', 'HTTPS_PROXY=', '--env', 'ALL_PROXY=',
                  '--env', 'NO_PROXY=*']

        def create(name):
            run('create', '--pull=never', '--name', name, *labels, *common, image['Id'])

        def wait_ready():
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                state = json.loads(run('inspect', primary).stdout)[0]['State']
                logs = run('logs', '--since', state['StartedAt'], primary).stdout
                if state['Status'] != 'running':
                    raise RuntimeError('Runtime container exited before ready: ' + logs[:1000])
                if 'management: http://127.0.0.1:5099' in logs:
                    return
                time.sleep(.1)
            raise RuntimeError('Runtime container did not become ready')

        def probe_api(phase):
            value = run('run', '--rm', '--pull=never', '--name', probe, *labels,
                '--network', 'container:' + primary, '--read-only', '--user', '10001:10001',
                '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--env', 'PYTHONDONTWRITEBYTECODE=1', '--env', 'HTTP_PROXY=',
                '--env', 'HTTPS_PROXY=', '--env', 'ALL_PROXY=', '--env', 'NO_PROXY=*',
                '--mount', 'type=bind,src=' + str(ROOT / 'tools') + ',dst=/probe,readonly',
                '--entrypoint', 'python3', probe_image['Id'], '/probe/container-probe.py',
                '--base', 'http://127.0.0.1:5099', '--phase', phase)
            return json.loads(value.stdout)

        try:
            run('volume', 'create', *labels, volume)
            create(primary)
            run('start', primary)
            wait_ready()
            details = json.loads(run('inspect', primary).stdout)[0]
            assert details['Image'] == image['Id']
            host = details['HostConfig']
            assert host['NetworkMode'] == 'none' and host['ReadonlyRootfs'] and not host['Privileged']
            assert not details['NetworkSettings']['Ports'] and not host['PortBindings']
            uid = int(run('exec', primary, 'id', '-u').stdout.strip())
            assert uid == 10001
            runtime_details = {'uid': uid, 'network_mode': host['NetworkMode'],
                'readonly_root': host['ReadonlyRootfs'], 'privileged': host['Privileged'],
                'cap_drop': host['CapDrop'], 'security_opt': host['SecurityOpt'],
                'published_ports': False, 'network_namespace': run('exec', primary, 'readlink', '/proc/self/ns/net').stdout.strip()}
            libraries = run('exec', primary, 'ldd', '/usr/local/bin/wispcat').stdout
            assert 'not found' not in libraries and 'libssl.so.3' in libraries and 'libsqlite3.so.0' in libraries
            package_versions = run('exec', primary, 'dpkg-query', '--show', '--showformat=${Package} ${Version}\n', 'libssl3', 'libsqlite3-0', 'ca-certificates').stdout
            binary = directory / 'wispcat'
            run('cp', primary + ':/usr/local/bin/wispcat', str(binary))
            version = binary_build_info(binary)
            assert 'CGO_ENABLED=1' in version and 'GOOS=linux' in version
            assert 'GOARCH=' + image['Architecture'] in version
            binary_metadata = {'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                               'build_info': version.replace(str(binary), 'wispcat')}
            first = probe_api('initialize')
            assert first['network_namespace'] == runtime_details['network_namespace']
            results.append(first)

            # A separate namespace must still refuse a second owner of the
            # same persistent state before starting any account worker.
            create(contender)
            run('start', contender)
            exit_code = int(run('wait', contender, timeout=20).stdout.strip())
            logs = run('logs', contender).stderr + run('logs', contender).stdout
            assert exit_code != 0 and 'state_root_in_use' in logs, logs

            run('stop', '--time', '15', primary, timeout=25)
            stopped.append(int(run('wait', primary).stdout.strip()))
            assert stopped[-1] == 0
            run('start', primary)
            wait_ready()
            second = probe_api('reopen')
            assert first['connections'] == second['connections'], 'saved connection revisions changed on restart'
            results.append(second)
            run('stop', '--time', '15', primary, timeout=25)
            stopped.append(int(run('wait', primary).stdout.strip()))
            assert stopped[-1] == 0
            result_report = {'result': 'pass', 'scope': 'actual Dockerfile runtime image, two empty synthetic offline accounts and separate loopback probe process; no real account/session or live platform worker',
                'image': {'requested': args.image, 'id': image['Id'], 'os': image['Os'],
                          'architecture': image['Architecture'], 'user': image['Config']['User']},
                'probe_image': {'id': probe_image['Id'], 'architecture': probe_image['Architecture']},
                'runtime': runtime_details, 'binary': binary_metadata, 'linked_libraries': libraries,
                'runtime_packages': package_versions, 'phases': results,
                'parallel_state_owner_rejected': True, 'persistent_config_reopened': True,
                'graceful_exit_codes': stopped, 'image_pulls_during_checks': 0,
                'live_wechat_or_model_requests': 0}
        finally:
            # Unique names plus ownership labels prevent touching unrelated
            # containers/volumes, including when a create had an unknown result.
            for name in containers:
                value = inspect_optional('container', name)
                if value is not None:
                    if value['Config']['Labels'].get(LABEL) != task:
                        raise RuntimeError('Refusing to clean an unowned container')
                    run('rm', '-f', name)
                    cleanup.append(name.rsplit('-', 1)[1])
            value = inspect_optional('volume', volume)
            if value is not None:
                if value['Labels'].get(LABEL) != task:
                    raise RuntimeError('Refusing to clean an unowned state volume')
                run('volume', 'rm', volume)
                cleanup.append('state-volume')
            if any(inspect_optional('container', name) is not None for name in containers) or inspect_optional('volume', volume) is not None:
                raise RuntimeError('Fixture cleanup incomplete')
    assert result_report is not None
    result_report['cleanup'] = cleanup
    result_report['elapsed_millis'] = int((time.monotonic() - started) * 1000)
    assert input_hashes == {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in input_paths}, 'Probe/Dockerfile changed during checks'
    result_report['dockerfile_sha256'] = input_hashes['Dockerfile']
    result_report['probe_source_sha256'] = input_hashes
    result_report['docker_engine'] = {'version': engine['Version'], 'architecture': engine['Arch']}
    result_report['architecture_emulated_on_this_engine'] = engine['Arch'] != image['Architecture']
    args.report.write_text(json.dumps(result_report, indent=2) + '\n')
    print(json.dumps({'result': 'pass', 'architecture': image['Architecture'],
                      'uid': runtime_details['uid'], 'offline_accounts': 2,
                      'state_reopen': True, 'parallel_owner_rejected': True}))


if __name__ == '__main__':
    main()
