"""Deterministic media corpus and bounded numeric-loopback reads for fixtures."""
import base64
import hashlib
import ipaddress
import json
import os
import struct
import zlib
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
    profile=os.environ.get('DEPLOYMENT_MEDIA_PROFILE','small');assert profile in ('small','large')
    large=profile=='large';size=CORPUS.get('large_payload_bytes',0)
    if large:assert 1<<20 <= size <= 16<<20
    if kind == 'image':
        body=base64.b64decode(CORPUS['image_base64'], validate=True)
        if not large:return body
        assert body[-8:]==b'IEND\xaeB`\x82'
        seed=('synthetic-image-padding/'+label).encode()
        count=size-len(body)-12;data=(seed*((count+len(seed)-1)//len(seed)))[:count]
        chunk=b'npAd'+data
        return body[:-12]+struct.pack('>I',len(data))+chunk+struct.pack('>I',zlib.crc32(chunk))+body[-12:]
    assert kind == 'file'
    body=CORPUS['file_template'].replace('{account}', label).encode()
    if large:assert body
    return (body*((size+len(body)-1)//len(body)))[:size] if large else body


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
                assert size <= 16 << 20
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

async def abandon(url):
    """Stop a real signed-URL response after one small prefix; never accept it."""
    local_media_url(url)
    async with httpx.AsyncClient(timeout=5,trust_env=False,follow_redirects=False) as client:
        async with client.stream('GET',url) as response:
            assert response.status_code==200
            expected=int(response.headers['content-length']);assert expected>1<<20
            iterator=response.aiter_bytes(chunk_size=16384)
            prefix=await anext(iterator)
            assert 0<len(prefix)<expected
            # Leaving this context closes the incomplete body and connection.
            return len(prefix)
