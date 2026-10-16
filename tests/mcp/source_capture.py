"""Bind the stdio probe and both executable builds without framework installs."""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tools'))
from source_manifest import runtime_inputs


def encode(value):
    return (json.dumps(value,sort_keys=True,indent=2)+'\n').encode()


def load_checked():
    value=json.loads((ROOT/'.bin/mcp-source.json').read_text())
    assert value=={'schema':1,'runtime_inputs':runtime_inputs(ROOT)},'MCP executable/probe inputs changed after build capture'
    return value,hashlib.sha256(encode(value)).hexdigest()


if __name__=='__main__':
    if '--check' in sys.argv:
        _,digest=load_checked()
    else:
        raw=encode({'schema':1,'runtime_inputs':runtime_inputs(ROOT)})
        digest=hashlib.sha256(raw).hexdigest()
        (ROOT/'.bin'/'reports').mkdir(parents=True, exist_ok=True)
        (ROOT/'.bin/mcp-source.json').write_bytes(raw)
        (ROOT/'.bin'/'reports'/('mcp-source-'+digest+'.json')).write_bytes(raw)
    print(digest)
