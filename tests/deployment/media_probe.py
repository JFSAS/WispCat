"""Deterministic media corpus and bounded numeric-loopback reads for fixtures."""
import base64
import hashlib
import ipaddress
import json
from pathlib import Path
import urllib.parse

import httpx

CORPUS = json.loads((Path(__file__).resolve().parents[2]/'internal/service/deployment-media.json').read_text())


def label_from_sender(name):
    prefix = 'Synthetic Friend '
    assert name.startswith(prefix), name
    label = name.removeprefix(prefix)
    assert label in ('synthetic-a', 'synthetic-b')
    return label


def payload(kind, label):
    if kind == 'image':
        return base64.b64decode(CORPUS['image_base64'], validate=True)
    assert kind == 'file'
    return CORPUS['file_template'].replace('{account}', label).encode()


def local_media_url(url):
    value = urllib.parse.urlparse(url)
    assert value.scheme == 'http' and value.hostname and ipaddress.ip_address(value.hostname).is_loopback
    assert not value.username and not value.password and not value.fragment and value.path == '/_media'
    assert value.port and set(urllib.parse.parse_qs(value.query)) == {'account','conversation','file','expires','signature'}


async def fetch(url):
    local_media_url(url)
    async with httpx.AsyncClient(timeout=5, trust_env=False, follow_redirects=False) as client:
        async with client.stream('GET', url) as response:
            assert response.status_code == 200, response.status_code
            chunks, size = [], 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                assert size <= 1 << 20
                chunks.append(chunk)
            return b''.join(chunks)


async def probe(caller, self_id, part, sender_name):
    kind, data = part['type'], part['data']
    assert kind in ('image','file') and str(data['file']).isdigit()
    content = await fetch(data['url'])
    assert content == payload(kind, label_from_sender(sender_name))
    if kind == 'image':
        info = await caller('get_image', self_id=self_id, file=str(data['file']))
        assert info['size'] == len(content) and info['file'] == str(data['file'])
        assert await fetch(info['url']) == content
    return content, {'media':kind, 'media_sha256':hashlib.sha256(content).hexdigest(),
                     'received_file_id':str(data['file'])}
