"""Capture executable inputs and the committed AstrBot tree, without network."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / '.bin/framework-sources/AstrBot'
REVISION = '9441402872f65fffc35685b0ead5bd3337033cb2'


def archive():
    env = dict(os.environ, GIT_NO_LAZY_FETCH='1', GIT_ALLOW_PROTOCOL='', GIT_TERMINAL_PROMPT='0')
    assert subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], env=env, text=True).strip() == REVISION
    subprocess.run(['git', '-C', str(SOURCE), 'diff', 'HEAD', '--exit-code', '--quiet'], env=env, check=True)
    return subprocess.check_output(['git', '-C', str(SOURCE), 'archive', '--format=tar', REVISION, 'astrbot'], env=env)


def capture():
    files = {}
    for name in ['cmd', 'internal', 'pkg', 'examples', 'tests', 'tools', 'provenance']:
        for path in sorted((ROOT/name).rglob('*')):
            if not path.is_file() or '__pycache__' in path.parts or path.name.startswith('._'):
                continue
            files[str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ['go.mod', 'go.sum']:
        files[name] = hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    return {'schema': 1, 'runtime_inputs': files, 'astrbot_revision': REVISION,
            'astrbot_archive_sha256': hashlib.sha256(archive()).hexdigest()}


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2)+'\n').encode()


def load_checked():
    value = json.loads((ROOT/'.bin/deployment-source.json').read_text())
    assert value == capture(), 'deployment input files changed since fixture build'
    return value, hashlib.sha256(encode(value)).hexdigest()


def freeze(temp, value):
    frozen = temp/'frozen'
    for name, digest in value['runtime_inputs'].items():
        if not (name.startswith('tests/deployment/') or name in ('tests/framework/stream_probe.py','internal/service/deployment-media.json')):
            continue
        raw = (ROOT/name).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == digest
        path = frozen/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o400)
    raw = archive()
    assert hashlib.sha256(raw).hexdigest() == value['astrbot_archive_sha256']
    external = frozen/'external/AstrBot'
    external.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as bundle:
        bundle.extractall(external, filter='data')
    for path in external.rglob('*'):
        if path.is_file() and not path.is_symlink():
            path.chmod(0o400)
    binary = temp/'deployment-fixture'
    descriptor = os.open(ROOT/'.bin/deployment-fixture', os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        body = source.read((200 << 20)+1)
    assert 0 < len(body) <= 200 << 20
    binary.write_bytes(body)
    binary.chmod(0o500)
    return frozen, external, binary, hashlib.sha256(body).hexdigest()


if __name__ == '__main__':
    if '--check' in sys.argv:
        _, digest = load_checked()
    else:
        raw = encode(capture())
        digest = hashlib.sha256(raw).hexdigest()
        (ROOT/'.bin/deployment-source.json').write_bytes(raw)
        (ROOT/'reports'/('deployment-source-'+digest+'.json')).write_bytes(raw)
    print(digest)
