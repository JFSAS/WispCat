"""Single-response upload acknowledgements over actual framework API callers.

Multi-frame downloads require a dedicated stream client and are not claimed as
supported by a framework's ordinary one-response call_api future.
"""
import base64
import hashlib


async def probe(caller, self_id, message_id):
    body = b'synthetic framework temporary transfer'
    stream_id = f'probe_{self_id}_{message_id}'
    ack = await caller('upload_file_stream', self_id=self_id, stream_id=stream_id,
        total_chunks=1, chunk_index=0, filename='fixture.bin',
        chunk_data=base64.b64encode(body).decode())
    assert ack['type'] == 'stream' and ack['received_chunks'] == 1, ack
    result = await caller('upload_file_stream', self_id=self_id, stream_id=stream_id,
        is_complete=True, file_size=len(body), expected_sha256=hashlib.sha256(body).hexdigest())
    assert result['type'] == 'response' and result['file_path'].startswith('stream://'), result
    assert result['file_size'] == len(body) and result['sha256'] == hashlib.sha256(body).hexdigest(), result
    cleanup = await caller('clean_stream_temp_file', self_id=self_id)
    assert cleanup['removed'] >= 2 and cleanup['skipped_active'] == 0, cleanup
    return True
