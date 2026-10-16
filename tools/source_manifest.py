"""Hash executable repository inputs, excluding private caches and reports."""
import hashlib


def runtime_inputs(root):
    files = {}
    for name in ['cmd', 'internal', 'pkg', 'examples', 'tests', 'tools', 'provenance']:
        for path in sorted((root/name).rglob('*')):
            if not path.is_file() or '__pycache__' in path.parts or path.name.startswith('._'):
                continue
            files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ['go.mod', 'go.sum']:
        files[name] = hashlib.sha256((root/name).read_bytes()).hexdigest()
    return files
