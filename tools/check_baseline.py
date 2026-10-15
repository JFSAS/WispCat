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
failures = []
implementations = tests = 0
for name, record in baseline['files'].items():
    file = root / name
    actual = hashlib.sha256(file.read_bytes()).hexdigest() if file.is_file() else 'missing'
    expected = record['imported_sha256']
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
          'intentional_test_changes': len(changes), 'source_checked': args.source is not None,
          'result': 'pass' if not failures else 'fail', 'failures': failures}
print(json.dumps(result, indent=2))
raise SystemExit(bool(failures))
