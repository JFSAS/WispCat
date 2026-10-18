"""Regression: runtime integration declarations cannot exempt protocol changes."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

source = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='wechat-baseline-verifier-') as raw:
    root = Path(raw)
    paths = set(json.loads((source/'provenance/main-baseline.json').read_text())['files'])
    paths.update({'tools/check_baseline.py', 'provenance/main-baseline.json',
                  'provenance/intentional-test-changes.json', 'provenance/runtime-integrations.json',
                  'provenance/runtime-recovery.patch'})
    for name in paths:
        target = root/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source/name, target)
    manifest = root/'provenance/runtime-integrations.json'
    valid = manifest.read_text()

    def check(expected):
        result = subprocess.run([sys.executable, str(root/'tools/check_baseline.py')], capture_output=True, text=True)
        assert (result.returncode == 0) == expected, (result.returncode, result.stdout, result.stderr)

    check(True)
    for kind in ['protocol_path', 'empty_insertion', 'duplicate_position', 'negative_position']:
        values = json.loads(valid)
        if kind == 'protocol_path':
            values['internal/protocol/client.go'] = {'reason': 'must not be allowed', 'insertions': []}
        else:
            item = values['internal/service/service.go']['insertions']
            if kind == 'empty_insertion':
                item[0]['text'] = ''
            elif kind == 'duplicate_position':
                item.append(item[0])
            else:
                item[0]['before_line'] = -1
        manifest.write_text(json.dumps(values))
        check(False)
    manifest.write_text(valid)
    for name in ['internal/protocol/client.go', 'internal/protocol/wire.go']:
        target = root/name
        original = target.read_bytes()
        target.write_bytes(original + b'\n// Unreviewed protocol change must fail.\n')
        check(False)
        target.write_bytes(original)
print('Baseline verifier regression: audited candidate accepted; 6 invalid candidates rejected')
