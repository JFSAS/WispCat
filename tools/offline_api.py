"""Loopback-only API checks shared by process and container fixtures."""
import ipaddress
import json
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        # Validate one destination only. Even another loopback origin must not
        # receive a token intended for the original listener.
        return None


def local_url(url):
    value = urllib.parse.urlparse(url)
    if value.scheme != 'http' or value.username or value.password or value.fragment or not value.hostname or not ipaddress.ip_address(value.hostname).is_loopback:
        raise ValueError('offline_probe_requires_numeric_loopback_http')


def request_json(url, token, method='GET', body=None):
    local_url(url)
    headers = {'Authorization': 'Bearer ' + token} if token else {}
    encoded = None
    if body is not None:
        encoded = json.dumps(body).encode()
        headers['Content-Type'] = 'application/json'
    request = urllib.request.Request(url, data=encoded, method=method, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(request, timeout=5) as response:
        raw = response.read((1 << 20) + 1)
        if len(raw) > 1 << 20:
            raise ValueError('offline_probe_response_limit')
        return json.loads(raw)


def probe_offline_accounts(base, admin_token, bot_token):
    rows = request_json(base + '/accounts', admin_token)
    assert len(rows) == 2, rows
    summaries = []
    for row in rows:
        status = row['status']
        assert status['mode'] == 'offline', status
        for counter in ['auth_calls', 'sync_calls', 'send_calls']:
            assert status[counter] == 0, status
        assert status['connection']['disk_read'] == 'not_attempted', status
        prefix = base + '/accounts/' + row['name']
        history = request_json(prefix + '/history', admin_token)
        assert history['format'] == 'encrypted_event_history_v2' and history['messages'] == 0 and history['cursor'] == 0, history
        streams = request_json(prefix + '/streams', admin_token)
        assert streams['file_count'] == 0 and streams['quota_bytes'] == 128 << 20 and streams['max_file_bytes'] == 16 << 20, streams
        incoming = request_json(prefix + '/media-cache', admin_token)
        assert incoming['format'] == 'encrypted_incoming_media_v1' and incoming['tasks'] == 0 and incoming['accounted_file_limit'] == 512, incoming
        outgoing = request_json(prefix + '/outgoing-media', admin_token)
        assert outgoing['format'] == 'encrypted_outgoing_media_v1' and outgoing['accounted_payload_bytes'] == 0 and outgoing['minimum_retention_seconds'] == 86400, outgoing
        response = request_json('http://' + row['onebot_address'] + '/get_status', bot_token)
        assert response['retcode'] == 0 and response['data']['online'] is False, response
        summaries.append({'name': row['name'], 'mode': status['mode'],
            **{key: status[key] for key in ['auth_calls', 'sync_calls', 'send_calls']},
            'disk_read': status['connection']['disk_read'], 'onebot_status': response['data'],
            'history': history, 'streams': streams, 'incoming_media': incoming, 'outgoing_media': outgoing})
    return summaries
