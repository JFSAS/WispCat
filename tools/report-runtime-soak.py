"""Capture source or summarize a completed timed synthetic runtime fixture."""
import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def source_manifest():
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(ROOT.rglob('*'))
            if path.is_file() and not {'.bin', '.git'}.intersection(path.relative_to(ROOT).parts)
            and not path.name.startswith('._')
            and (path.suffix in {'.go', '.c', '.h'} or path.name in {'go.mod', 'go.sum'})}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    capture = commands.add_parser('capture')
    capture.add_argument('output', type=Path)
    summary = commands.add_parser('summarize')
    summary.add_argument('input', type=Path)
    summary.add_argument('--manifest', required=True, type=Path)
    summary.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    if args.command == 'capture':
        with args.output.open('x') as output:
            output.write(json.dumps(source_manifest(), indent=2) + '\n')
        return
    manifest = json.loads(args.manifest.read_text())
    if manifest != source_manifest():
        raise SystemExit('Source changed after soak capture; do not relabel the measured result')
    raw = args.input.read_bytes()
    events = [json.loads(line) for line in raw.splitlines()]
    if any(event.get('Action') in {'fail', 'build-fail'} for event in events):
        raise SystemExit('Timed runtime fixture failed')
    target = 'TestRuntimeOfflineSoak'
    finished = [event for event in events if event.get('Test') == target and event.get('Action') == 'pass']
    if len(finished) != 1:
        raise SystemExit('Missing a single completed runtime fixture')
    package = finished[0]['Package']
    module = next(line.split()[1] for line in (ROOT / 'go.mod').read_text().splitlines() if line.startswith('module '))
    if package != module + '/internal/service':
        raise SystemExit('Unexpected fixture package')
    runs = [i for i, event in enumerate(events) if event.get('Package') == package and event.get('Test') == target and event.get('Action') == 'run']
    starts = [i for i, event in enumerate(events) if event.get('Package') == package and not event.get('Test') and event.get('Action') == 'start']
    ends = [i for i, event in enumerate(events) if event.get('Package') == package and not event.get('Test') and event.get('Action') in {'pass', 'fail', 'skip'}]
    test_end = events.index(finished[0])
    if len(runs) != 1 or len(starts) != 1 or len(ends) != 1 or events[ends[0]]['Action'] != 'pass' or not starts[0] < runs[0] < test_end < ends[0]:
        raise SystemExit('Missing a single completed successful package run')
    # go test -json may split one long t.Log line across several Output events.
    output = ''.join(event.get('Output', '') for event in events if event.get('Test') == target)
    marker = 'RUNTIME_SOAK_REPORT='
    if output.count(marker) != 1:
        raise SystemExit('Missing or duplicate fixture payload')
    result, _ = json.JSONDecoder().raw_decode(output.split(marker, 1)[1])
    manifest_bytes = args.manifest.read_bytes()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if result.get('source_manifest_sha256') != manifest_sha:
        raise SystemExit('Fixture binary is not bound to this source capture')
    configured = result['configured_duration_millis']
    if result['elapsed_millis'] < configured or finished[0]['Elapsed'] * 1000 + 100 < result['elapsed_millis']:
        raise SystemExit('Fixture duration inconsistent with completed Go test')
    if len(result['accounts']) != 2 or result['planned_restarts'] != 3 or result['production_session_worker'] or result['live_wechat_or_model_requests'] != 0:
        raise SystemExit('Unexpected fixture scope')
    if result['phases'] != 4 or not all(result.get(key) is True for key in ['oldest_queries_and_idempotency_after_reopen', 'cross_account_public_id_fencing', 'temporary_references_rejected_after_reopen']):
        raise SystemExit('Missing fixture invariant evidence')
    for account in result['accounts']:
        if account['events'] < 1 or account['events'] != account['synthetic_native_receipts']:
            raise SystemExit('Events/replies did not balance')
        if configured >= 60000 and (account['stream_roundtrips'] < 1 or account['heartbeats'] < 1):
            raise SystemExit('Timed run did not exercise streams and heartbeats')
    published = ROOT / 'reports' / ('runtime-soak-source-' + manifest_sha + '.json')
    try:
        with published.open('xb') as output:
            output.write(manifest_bytes)
    except FileExistsError:
        if published.read_bytes() != manifest_bytes:
            raise SystemExit('Published source artifact changed')
    result.update({'result': 'pass', 'default_suite': False,
                   'consumer_process_scope': 'SDK consumers are goroutines in the same fixture process, using actual loopback HTTP and Event WebSocket; not a separate full application deployment',
                   'test_json_sha256': hashlib.sha256(raw).hexdigest(),
                   'source_manifest_path': str(published.relative_to(ROOT)),
                   'source_manifest_sha256': manifest_sha,
                   'source_files_hashed': len(manifest),
                   'resource_sample_interpretation': 'quiescent per-phase process observations; no memory envelope or production throughput claim'})
    args.report.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'result': 'pass', 'configured_duration_millis': configured,
                      'elapsed_millis': result['elapsed_millis'],
                      'events_per_account': [a['events'] for a in result['accounts']]}))


if __name__ == '__main__':
    main()
