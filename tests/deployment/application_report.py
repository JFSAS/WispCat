"""Atomic fixture observations shared by the actual loaded plugin and peer."""
import json
import os
from pathlib import Path

REPORT=Path(os.environ['FRAMEWORK_REPORT'])


def read_report():return json.loads(REPORT.read_text())


def write_report(value):
    temporary=REPORT.with_suffix('.pending')
    temporary.write_text(json.dumps(value,indent=2)+'\n');temporary.chmod(0o600)
    temporary.replace(REPORT)
