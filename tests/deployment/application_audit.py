"""Exact allowlist for the pinned upstream's local no-op sandbox probe."""
import os
from pathlib import Path


def is_noop_sandbox_probe(args, root, build_command):
    executable, argv, cwd, env = args
    if executable != '/usr/bin/sandbox-exec' or not isinstance(argv, (list, tuple)):
        return False
    if len(argv) < 3 or list(argv[:2]) != ['/usr/bin/sandbox-exec', '-D']:
        return False
    if not isinstance(argv[2], str) or not argv[2].startswith('WORKSPACE='):
        return False
    workspace = Path(argv[2].removeprefix('WORKSPACE='))
    parent = root.resolve()/'data/temp'
    if (not workspace.is_absolute() or workspace != workspace.resolve()
            or workspace.parent != parent or not workspace.is_dir()
            or not workspace.name.startswith('sandbox-probe-')
            or workspace.name == 'sandbox-probe-'):
        return False
    if not isinstance(cwd, (str, Path)) or os.fspath(cwd) != str(workspace) or env != {'PATH': os.defpath}:
        return False
    # The callback rebuilds the pinned upstream command with its default
    # SandboxSpec: workspace scope, no network, default limits, no extra roots
    # or environment. Equality binds policy, wrapper code and every argument.
    return list(argv) == build_command(workspace)
