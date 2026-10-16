"""Import the pinned full lifecycle in a disposable root; never stub imports."""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / '.bin/framework-sources/AstrBot'
REVISION = '9441402872f65fffc35685b0ead5bd3337033cb2'
assert subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip() == REVISION
subprocess.run(['git', '-C', str(SOURCE), 'diff', 'HEAD', '--exit-code', '--quiet'], check=True)
with tempfile.TemporaryDirectory(prefix='astrbot-full-preflight-') as directory:
    os.environ['HOME'] = directory
    os.environ['ASTRBOT_ROOT'] = directory
    os.environ['ASTRBOT_DISABLE_METRICS'] = '1'
    os.chdir(directory)
    sys.path.insert(0, str(SOURCE))
    try:
        from astrbot.core.core_lifecycle import AstrBotCoreLifecycle
        result = {'result': 'import_only', 'module': AstrBotCoreLifecycle.__module__,
                  'application_started': False}
    except ModuleNotFoundError as error:
        result = {'result': 'unavailable', 'missing_module': error.name,
                  'application_started': False}
    result.update(scope='Full lifecycle import preflight, not a passed application integration',
        source_revision=REVISION, python_version=platform.python_version(),
        installed_adapter_dependencies={name: importlib.metadata.version(name) for name in ['aiocqhttp','quart','hypercorn']},
        adapter_dependency_lock_sha256=hashlib.sha256((ROOT/'tests/framework/astrbot-requirements.lock').read_bytes()).hexdigest(),
        dependency_downloads=False, live_wechat_or_model_requests=0)
    (ROOT/'reports/astrbot-application-preflight.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))
    if result['result'] == 'unavailable':
        sys.exit(2)
