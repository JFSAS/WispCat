"""Run native race tests in a prepared Dockerfile build stage, network disabled."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parent.parent
LABEL = 'org.wechat-runtime.offline-go-tests'


def manifest(directory):
    values = {}
    for path in sorted(directory.rglob('*')):
        relative = path.relative_to(directory)
        if not path.is_file() or {'.bin', '.git'}.intersection(relative.parts) or path.name.startswith('._'):
            continue
        # Include whole source trees, not just familiar resource directories:
        # go:embed also uses catalog JSON and the CDN's DER public key.
        if path.suffix in {'.go', '.c', '.h'} or path.name in {'go.mod', 'go.sum'} or relative.parts[0] in {'cmd', 'internal', 'pkg', 'provenance'}:
            values[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return values


def require_matching_source(directory, expected):
    actual = manifest(directory)
    if actual != expected:
        changed = sorted(path for path in actual.keys() | expected.keys()
                         if actual.get(path) != expected.get(path))
        raise ValueError('Builder sources/resources differ from current checkout: ' + ', '.join(changed[:5]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True)
    parser.add_argument('--builder-image', required=True)
    parser.add_argument('--runtime-image', required=True)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    docker = ['docker', '--context', args.context]

    def run(*words, check=True, timeout=60):
        result = subprocess.run(docker + list(words), capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError('docker command failed: ' + ' '.join(words[:2]) + ': ' + result.stderr[:1000])
        return result

    def optional_container(container):
        value = run('inspect', container, check=False)
        if value.returncode == 0:
            return json.loads(value.stdout)[0]
        if 'no such object' not in value.stderr.lower() and 'no such container' not in value.stderr.lower():
            raise RuntimeError('Cannot confirm test container state: ' + value.stderr[:300])
        return None

    endpoint = json.loads(run('context', 'inspect', args.context).stdout)[0]['Endpoints']['docker']['Host']
    assert endpoint.startswith('unix://'), 'local Docker Unix socket required'
    builder = json.loads(run('image', 'inspect', args.builder_image).stdout)[0]
    runtime_image = json.loads(run('image', 'inspect', args.runtime_image).stdout)[0]
    assert builder['Os'] == runtime_image['Os'] == 'linux' and builder['Architecture'] == runtime_image['Architecture']
    architecture = builder['Architecture']
    task = uuid.uuid4().hex
    name = 'wechat-runtime-go-' + task
    extraction = name + '-artifact'
    (ROOT / '.bin').mkdir(exist_ok=True)
    raw_path = ROOT / '.bin' / (name + '.jsonl')
    log_path = ROOT / '.bin' / (name + '.log')
    started = time.monotonic()
    source = manifest(ROOT)
    result = None
    checker_paths = [ROOT / 'tools/container-go-checks.py', ROOT / 'tools/report-go-tests.py']
    checker_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in checker_paths}
    dockerfile_sha = hashlib.sha256((ROOT / 'Dockerfile').read_bytes()).hexdigest()
    engine = json.loads(run('version', '--format', '{{json .Server}}').stdout)
    with tempfile.TemporaryDirectory(prefix='container-go-source-', dir=ROOT / '.bin') as raw:
        directory = Path(raw)
        try:
            run('create', '--pull=never', '--name', name, '--label', LABEL + '=' + task,
                '--network', 'none', '--memory', '2g', '--cpus', '2', '--cap-drop', 'NET_RAW',
                '--env', 'GOPROXY=off', '--env', 'GOSUMDB=off', '--env', 'GOTOOLCHAIN=local',
                '--env', 'GOENV=off', '--env', 'CGO_ENABLED=1', builder['Id'],
                'sh', '-c', 'go version >&2; exec go test -race -count=1 -timeout 30m -json ./...')
            run('cp', name + ':/src', str(directory))
            require_matching_source(directory / 'src', source)
            binary = directory / 'built-runtime'
            run('cp', name + ':/out/wechat-runtime', str(binary))
            built_sha = hashlib.sha256(binary.read_bytes()).hexdigest()
            # Inspect the runtime artifact in an unstarted, network-disabled
            # temporary container; do not run a second state owner.
            run('create', '--pull=never', '--name', extraction, '--label', LABEL + '=' + task,
                '--network', 'none', runtime_image['Id'], 'catalog')
            try:
                actual = directory / 'runtime-image-binary'
                run('cp', extraction + ':/usr/local/bin/wechat-runtime', str(actual))
                assert hashlib.sha256(actual.read_bytes()).hexdigest() == built_sha, 'Test builder is not the producer of this runtime binary'
            finally:
                run('rm', extraction)
            inspected = json.loads(run('inspect', name).stdout)[0]
            assert inspected['Image'] == builder['Id'] and inspected['HostConfig']['NetworkMode'] == 'none'
            with raw_path.open('w') as output, log_path.open('w') as errors:
                attached = subprocess.run(docker + ['start', '-a', name], stdout=output, stderr=errors, timeout=1900)
            state = json.loads(run('inspect', name).stdout)[0]['State']
            assert attached.returncode == 0 and state['ExitCode'] == 0 and not state['OOMKilled'], state
            assert manifest(ROOT) == source, 'Source changed during container test execution'
            summary_path = directory / 'summary.json'
            subprocess.run([sys.executable, str(ROOT / 'tools/report-go-tests.py'), str(raw_path), '--report', str(summary_path)],
                           check=True, capture_output=True, text=True)
            summary = json.loads(summary_path.read_text())
            assert summary['result'] == 'pass'
            manifest_bytes = (json.dumps(source, indent=2) + '\n').encode()
            manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
            published = ROOT / '.bin' / 'reports' / ('container-go-source-' + manifest_sha + '.json')
            published.parent.mkdir(parents=True, exist_ok=True)
            if published.exists():
                assert published.read_bytes() == manifest_bytes
            else:
                with published.open('xb') as output:
                    output.write(manifest_bytes)
            assert hashlib.sha256((ROOT / 'Dockerfile').read_bytes()).hexdigest() == dockerfile_sha, 'Dockerfile changed during checks'
            assert checker_hashes == {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in checker_paths}, 'Checker changed during execution'
            result = {'result': 'pass', 'scope': 'uncached race-enabled native Go suite in prepared Dockerfile build image with no external network; production runtime binary matches tested build artifact',
                'architecture': architecture, 'network_mode': 'none', 'test_user': builder['Config'].get('User') or 'root',
                'builder_image_id': builder['Id'], 'runtime_image_id': runtime_image['Id'],
                'runtime_binary_sha256': built_sha, 'toolchain_log': log_path.read_text()[:2000],
                'source_manifest_path': str(published.relative_to(ROOT)),
                'source_manifest_sha256': hashlib.sha256(published.read_bytes()).hexdigest(),
                'source_files_hashed': len(source), 'source_scope': 'Go/C/header/module files, complete cmd/internal/pkg trees and provenance records',
                'test_summary': summary, 'test_json_sha256': hashlib.sha256(raw_path.read_bytes()).hexdigest(),
                'dockerfile_sha256': dockerfile_sha,
                'checker_source_sha256': checker_hashes,
                'docker_engine': {'version': engine['Version'], 'architecture': engine['Arch']},
                'architecture_emulated_on_this_engine': engine['Arch'] != architecture,
                'image_pulls_during_checks': 0, 'dependency_downloads_during_checks': 0,
                'live_wechat_or_model_requests': 0, 'elapsed_millis': int((time.monotonic()-started)*1000)}
        finally:
            for container in [extraction, name]:
                value = optional_container(container)
                if value is not None:
                    assert value['Config']['Labels'].get(LABEL) == task, 'Refusing to clean an unowned container'
                    run('rm', '-f', container)
                assert optional_container(container) is None, 'Test container cleanup incomplete'
    assert result is not None
    result['container_removed'] = True
    args.report.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'result': 'pass', 'architecture': architecture,
        'tests_passed': result['test_summary']['top_level_passed'],
        'tests_skipped': result['test_summary']['top_level_skipped']}))


if __name__ == '__main__':
    main()
