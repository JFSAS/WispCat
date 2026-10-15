#!/usr/bin/env python3
"""Check the frozen main import; never regenerate expected hashes implicitly."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--source', type=Path, help='Optional original checkout; read-only git-show verification')
args = parser.parse_args()
baseline = json.loads((root / 'provenance/main-baseline.json').read_text())
changes = json.loads((root / 'provenance/intentional-test-changes.json').read_text())
integrations = json.loads((root / 'provenance/runtime-integrations.json').read_text())
failures = []
allowed_integrations = {
    'internal/service/service.go', 'internal/service/channel.go',
    'internal/service/media_transfer.go', 'internal/service/login.go',
}
if set(integrations) != allowed_integrations:
    failures.append('runtime integrations must match the four audited service paths')
for name, declaration in integrations.items():
    positions = []
    if not declaration.get('reason') or not declaration.get('insertions'):
        failures.append(name + ': missing insertion explanation or content')
    for insertion in declaration.get('insertions', []):
        line = insertion.get('before_line')
        if type(line) is not int or line < 1 or not insertion.get('text') or not insertion['text'].endswith('\n'):
            failures.append(name + ': invalid insertion location/content')
        positions.append(line)
    if any(type(line) is not int for line in positions) or positions != sorted(set(positions)):
        failures.append(name + ': insertion positions must be unique and ordered')
if failures:
    print(json.dumps({'result': 'fail', 'failures': failures}, indent=2))
    raise SystemExit(1)
implementations = tests = 0
for name, record in baseline['files'].items():
    file = root / name
    actual = hashlib.sha256(file.read_bytes()).hexdigest() if file.is_file() else 'missing'
    expected = record['imported_sha256']
    if name in integrations and file.is_file():
        lines = file.read_bytes().splitlines(keepends=True)
        offset = 0
        for insertion in integrations[name]['insertions']:
            added = insertion['text'].encode().splitlines(keepends=True)
            position = insertion['before_line'] - 1 + offset
            if lines[position:position + len(added)] != added:
                failures.append(name + ': declared runtime insertion differs')
            offset += len(added)
        # Remove only the declared additions at their exact source positions.
        # The original expected hash stays frozen and every original byte must
        # remain. This cannot hide a modified/deleted/reordered baseline line.
        for insertion in reversed(integrations[name]['insertions']):
            earlier = sum(len(row['text'].encode().splitlines(keepends=True))
                          for row in integrations[name]['insertions']
                          if row['before_line'] < insertion['before_line'])
            added = insertion['text'].encode().splitlines(keepends=True)
            position = insertion['before_line'] - 1 + earlier
            del lines[position:position + len(added)]
        actual = hashlib.sha256(b''.join(lines)).hexdigest()
    if name in changes:
        if not name.endswith('_test.go'):
            failures.append(name + ': implementation exceptions are forbidden')
        expected = changes[name]['sha256']
    if actual != expected:
        failures.append(name + ': imported baseline differs')
    if name.endswith('_test.go'):
        tests += 1
    else:
        implementations += 1
    if args.source:
        raw = subprocess.check_output(['git', 'show', baseline['commit'] + ':' + name], cwd=args.source)
        if hashlib.sha256(raw).hexdigest() != record['source_sha256']:
            failures.append(name + ': original source hash differs')
        normalized = raw.replace(baseline['module_before'].encode(), baseline['module_after'].encode())
        if hashlib.sha256(normalized).hexdigest() != record['imported_sha256']:
            failures.append(name + ': module rewrite differs')
result = {'baseline_commit': baseline['commit'], 'imported_files': len(baseline['files']),
          'implementation_files': implementations, 'test_files': tests,
          'identical_implementation_files': implementations - len(integrations),
          'runtime_integration_files': len(integrations),
          'intentional_test_changes': len(changes), 'source_checked': args.source is not None,
          'result': 'pass' if not failures else 'fail', 'failures': failures}
print(json.dumps(result, indent=2))
raise SystemExit(bool(failures))
