"""Consumer-owned durable request identity for the lost-response fixture."""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import time

from media_probe import label_from_sender, payload

PREFIX = 'synthetic-receipt-loss/'


def enabled(text):
    return os.environ.get('DEPLOYMENT_RECEIPT_LOSS') == '1' and text in ('/receipt-send', '/receipt-retry')


def parameters(record):
    body = payload('file', record['label'])
    assert len(body) == record['bytes'] and hashlib.sha256(body).hexdigest() == record['sha256']
    return {'self_id': record['self_id'], 'user_id': record['user_id'],
            'wechat_request_id': record['request_id'],
            'message': [{'type': 'file', 'data': {'file': 'base64://' + base64.b64encode(body).decode(), 'name': 'reply.bin'}}]}


def request_for(text, self_id, user_id, sender):
    assert enabled(text)
    path = Path(os.environ['DEPLOYMENT_RECEIPT_RECORD'])
    label = label_from_sender(sender)
    if text == '/receipt-send':
        assert not path.exists()
        body = payload('file', label)
        record = {'schema': 1, 'request_id': PREFIX + secrets.token_hex(16),
                  'self_id': int(self_id), 'user_id': int(user_id), 'label': label,
                  'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
        temporary = path.with_suffix('.pending')
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as output:
            json.dump(record, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        info = os.fstat(source.fileno())
        assert stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
        raw = source.read(4097)
    assert len(raw) <= 4096
    record = json.loads(raw)
    assert record['schema'] == 1 and record['request_id'].startswith(PREFIX)
    assert (record['self_id'], record['user_id'], record['label']) == (int(self_id), int(user_id), label)
    return parameters(record), {**record, 'record_sha256': hashlib.sha256(raw).hexdigest()}


def completion(command, outcome, error=None):
    assert command in ('/receipt-send', '/receipt-retry')
    assert outcome in ('success', 'error') and (error is not None) == (outcome == 'error')
    value = {'command': command, 'outcome': outcome, 'completed_monotonic': time.monotonic()}
    if error is not None:
        value['category'] = type(error).__name__
    return value


def original_completion(rows, command_at, api_timeout):
    matches = [row for row in rows if row['command'] == '/receipt-send']
    if not matches:
        return None
    assert len(matches) == 1, 'duplicate original API completion'
    value = matches[0]
    assert value['outcome'] == 'error', 'withheld original reply reported success'
    finished = value['completed_monotonic']
    assert type(finished) in (int, float) and 0 <= finished-command_at <= api_timeout+15, 'original API completed outside its own deadline'
    return value
