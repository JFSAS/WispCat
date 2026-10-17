"""Synthetic negative probes for evidence parsing and network inspection gates.

These are tooling regressions, not runtime or platform execution evidence.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile


ROOT = Path(__file__).resolve().parent.parent
PARSER = ROOT / 'tools/report-runtime-soak.py'
GUARD = ROOT / 'tools/require-loopback-network.sh'


def main():
    (ROOT / '.bin').mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='evidence-probes-', dir=ROOT / '.bin') as raw:
        directory = Path(raw)
        project = directory / 'synthetic-project'
        (project / 'tools').mkdir(parents=True)
        (project / '.bin' / 'reports').mkdir(parents=True)
        (project / 'go.mod').write_text('module github.com/JFSAS/wispcat\n\ngo 1.25\n')
        (project / 'fixture.go').write_text('package fixture\n')
        fixture_parser = project / 'tools/report-runtime-soak.py'
        shutil.copyfile(PARSER, fixture_parser)
        manifest = directory / 'source.json'
        subprocess.run([sys.executable, str(fixture_parser), 'capture', str(manifest)], check=True)
        sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
        payload = {'configured_duration_millis': 8000, 'elapsed_millis': 8100,
                   'accounts': [{'events': 1, 'synthetic_native_receipts': 1}] * 2,
                   'planned_restarts': 3, 'phases': 4, 'production_session_worker': False,
                   'live_wechat_or_model_requests': 0, 'source_manifest_sha256': sha,
                   'oldest_queries_and_idempotency_after_reopen': True,
                   'cross_account_public_id_fencing': True,
                   'temporary_references_rejected_after_reopen': True}
        package = 'github.com/JFSAS/wispcat/internal/service'
        target = 'TestRuntimeOfflineSoak'
        output = 'RUNTIME_SOAK_REPORT=' + json.dumps(payload) + '\n'
        events = [
            {'Package': package, 'Action': 'start'},
            {'Package': package, 'Test': target, 'Action': 'run'},
            {'Package': package, 'Test': target, 'Action': 'output', 'Output': output[:120]},
            {'Package': package, 'Test': target, 'Action': 'output', 'Output': output[120:]},
            {'Package': package, 'Test': target, 'Action': 'pass', 'Elapsed': 8.1},
            {'Package': package, 'Action': 'pass', 'Elapsed': 8.2}]

        def summarize(rows, name, success):
            path = directory / (name + '.jsonl')
            report = directory / (name + '.json')
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            result = subprocess.run([sys.executable, str(fixture_parser), 'summarize', str(path),
                                     '--manifest', str(manifest), '--report', str(report)],
                                    capture_output=True, text=True)
            if (result.returncode == 0) != success or report.exists() != success:
                raise AssertionError((name, result.returncode, result.stdout, result.stderr))

        summarize(events, 'complete-synthetic-output', True)
        summarize(events[:-1], 'test-pass-without-package-completion', False)
        summarize(events + [{'Package': package, 'Action': 'fail'}], 'late-package-failure', False)
        summarize(events + events, 'duplicate-runs', False)
        wrong = [dict(row) for row in events]
        wrong[2]['Output'] = output.replace(sha, '0' * 64)
        wrong[3]['Output'] = ''
        summarize(wrong, 'different-binary-source-binding', False)
        manifest_data = json.loads(manifest.read_text())
        first = next(iter(manifest_data))
        manifest_data[first] = '0' * 64
        manifest.write_text(json.dumps(manifest_data))
        summarize(events, 'changed-current-source-capture', False)
        retry = subprocess.run([sys.executable, str(fixture_parser), 'capture', str(manifest)], capture_output=True)
        assert retry.returncode != 0, 'existing capture was overwritten'

        # Fake inspection commands never run Go. Actual positive namespace
        # validation is performed separately with the real Linux iproute2.
        commands = directory / 'commands'
        commands.mkdir()
        (commands / 'uname').write_text('#!/bin/sh\necho Linux\n')
        (commands / 'uname').chmod(0o700)
        (commands / 'python3').symlink_to(sys.executable)
        env = {**os.environ, 'PATH': str(commands)}
        missing = subprocess.run(['/bin/sh', str(GUARD)], env=env, capture_output=True)
        assert missing.returncode != 0 and b'iproute2' in missing.stderr
        links = [{'ifname': 'lo', 'flags': ['UP', 'LOOPBACK']}]

        def inspect(name, replies, expected):
            script = ['#!/bin/sh', 'case "$*" in']
            for args, (status, stdout) in replies.items():
                quoted = "'" + stdout.replace("'", "'\\''") + "'"
                script.append(f'"{args}") printf %s {quoted}; exit {status};;')
            script.extend(['*) exit 1;;', 'esac'])
            (commands / 'ip').write_text('\n'.join(script) + '\n')
            (commands / 'ip').chmod(0o700)
            result = subprocess.run(['/bin/sh', str(GUARD)], env=env, capture_output=True)
            assert (result.returncode == 0) == expected, (name, result.stderr)

        empty = {'-j link show': (0, json.dumps(links)), '-o address show scope global': (0, ''),
                 'route show': (0, ''), '-6 route show default': (0, '')}
        inspect('synthetic-loopback', empty, True)
        for query in empty:
            broken = dict(empty)
            broken[query] = (1, '')
            inspect('failed-' + query, broken, False)
        external = dict(empty)
        external['-j link show'] = (0, json.dumps(links + [{'ifname': 'eth0', 'flags': ['UP']}]))
        inspect('external-interface', external, False)
        external = dict(empty)
        external['route show'] = (0, 'default via 192.0.2.1 dev eth0')
        inspect('external-route', external, False)
        fallback = dict(empty)
        tunnel = {'ifname': 'tunl0', 'link_type': 'ipip', 'flags': ['NOARP'], 'operstate': 'DOWN', 'link': None}
        fallback['-j link show'] = (0, json.dumps(links + [tunnel]))
        inspect('down-kernel-fallback', fallback, True)
        fallback['-j link show'] = (0, json.dumps(links + [{**tunnel, 'flags': ['UP']}]))
        inspect('active-fallback', fallback, False)
        print('Evidence parser: complete fragmented output accepted; five invalid runs and capture overwrite rejected')
        print('Network gate: synthetic loopback/down fallback accepted; missing tool, four query failures, external link/route and active fallback rejected')


if __name__ == '__main__':
    main()
