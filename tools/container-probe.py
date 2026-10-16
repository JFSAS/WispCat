"""Run inside a separate Python container sharing the offline runtime network."""
import argparse
import json
import os
import socket
from pathlib import Path
import urllib.error

from offline_api import probe_offline_accounts, request_json


ADMIN = 'synthetic-admin-token'
BOT = 'synthetic-onebot-token'


def expect_unauthorized(url):
    try:
        request_json(url, '')
    except urllib.error.HTTPError as error:
        assert error.code == 401, error.code
    else:
        raise AssertionError('unauthenticated container API access succeeded')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True)
    parser.add_argument('--phase', choices=['initialize', 'reopen'], required=True)
    args = parser.parse_args()
    active = sorted(name for _, name in socket.if_nameindex()
                    if int((Path('/sys/class/net') / name / 'flags').read_text().strip(), 16) & 1)
    assert active == ['lo'], active
    for line in Path('/proc/net/route').read_text().splitlines()[1:]:
        assert not line.strip() or line.split()[0] == 'lo', 'external IPv4 route'
    for line in Path('/proc/net/ipv6_route').read_text().splitlines():
        assert not line.strip() or line.split()[-1] == 'lo', 'external IPv6 route'
    expect_unauthorized(args.base + '/accounts')
    summaries = probe_offline_accounts(args.base, ADMIN, BOT)
    storage = request_json(args.base + '/storage', ADMIN)
    assert storage['identities'] == 0 and storage['file_bytes'] > 0, storage
    for row in request_json(args.base + '/accounts', ADMIN):
        base = 'http://' + row['onebot_address']
        expect_unauthorized(base + '/get_status')
        catalog = request_json(base + '/_actions', BOT)
        assert len(catalog) == 22 and {'get_status', 'send_private_msg', 'upload_file_stream'}.issubset({item['name'] for item in catalog}), catalog
    connections = []
    for account in summaries:
        prefix = args.base + '/accounts/' + account['name'] + '/connections'
        state = request_json(prefix, ADMIN)
        if args.phase == 'initialize':
            request_json(prefix, ADMIN, 'POST', {'revision': state['revision'], 'plan': {'reverse': [], 'http_post': []}})
            request_json(prefix + '/apply', ADMIN, 'POST', {})
            state = request_json(prefix, ADMIN)
        assert state['saved'] == {'reverse': [], 'http_post': []}, state
        assert state['applied'], state
        connections.append({'account': account['name'], 'revision': state['revision'], 'applied': state['applied']})
    print(json.dumps({'result': 'pass', 'phase': args.phase, 'probe_uid': os.getuid(),
                     'active_interfaces': active, 'network_namespace': os.readlink('/proc/self/ns/net'),
                     'accounts': summaries, 'connections': connections,
                     'catalog_actions_per_account': 22, 'storage': storage,
                     'live_wechat_or_model_requests': 0}))


if __name__ == '__main__':
    main()
