"""Summarize actual go test -json output without treating skips as passes."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('input', type=Path)
parser.add_argument('--report', type=Path)
args = parser.parse_args()
raw = args.input.read_bytes()
events = [json.loads(line) for line in raw.splitlines()]
failed = [event for event in events if event.get('Action') == 'fail']
if failed:
    raise SystemExit('Go tests failed: ' + json.dumps(failed))
cases = [event for event in events if event.get('Test') and '/' not in event['Test']]
passed = sorted({(event['Package'], event['Test']) for event in cases if event['Action'] == 'pass'})
skipped = sorted({(event['Package'], event['Test']) for event in cases if event['Action'] == 'skip'})
packages = sorted({event['Package'] for event in events if event['Action'] == 'pass' and not event.get('Test')})
assert passed and packages, 'No completed test packages were recorded'
loads = [json.loads(event['Output'].split('STATE_INDEX_LOAD_REPORT=', 1)[1])
         for event in events if 'STATE_INDEX_LOAD_REPORT=' in event.get('Output', '')]
result = {'result': 'pass', 'top_level_passed': len(passed), 'top_level_skipped': len(skipped),
          'skipped': skipped, 'passed_packages': packages, 'index_load': loads,
          'test_json_sha256': hashlib.sha256(raw).hexdigest()}
if args.report:
    args.report.write_text(json.dumps(result, indent=2) + '\n')
print(f'Passed: {len(passed)} top-level tests; skipped: {len(skipped)}; packages: {len(packages)}')
for load in loads:
    print(json.dumps(load))
