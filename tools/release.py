#!/usr/bin/env python3
"""Build, relocate, verify and publish native macOS/Linux release bundles."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
TARGETS = {('darwin', 'arm64'), ('darwin', 'amd64'), ('linux', 'amd64'), ('linux', 'arm64')}
SYSTEM_LINUX = {'linux-vdso.so.1', 'libc.so.6', 'libm.so.6', 'libdl.so.2',
                'libpthread.so.0', 'libresolv.so.2', 'librt.so.1',
                'ld-linux-x86-64.so.2', 'ld-linux-aarch64.so.1'}
BUNDLED_LINUX = {'libssl.so.3', 'libcrypto.so.3', 'libsqlite3.so.0'}
TAG = re.compile(r'v(\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?)\Z')


def run(*args, env=None, cwd=ROOT, capture=True, input=None, timeout=600):
    result = subprocess.run([str(a) for a in args], cwd=cwd, env=env,
                            text=True, capture_output=capture, check=True, input=input, timeout=timeout)
    return result.stdout.strip() if capture else ''


def version(tag):
    match = TAG.fullmatch(tag)
    if not match:
        raise ValueError('release tag must be vMAJOR.MINOR.PATCH[-prerelease]')
    return match[1]


def source(tag):
    value = version(tag)
    assert json.loads((ROOT/'webui/package.json').read_text())['version'] == value, 'tag/package version mismatch'
    for name in ['internal/onebot/runtime.go', 'internal/mcp/server.go']:
        assert f'"{value}"' in (ROOT/name).read_text(), f'{name}: version mismatch'
    assert not run('git', 'status', '--porcelain'), 'release source must be a clean checkout'
    return {'tag': tag, 'version': value, 'commit': run('git', 'rev-parse', 'HEAD'),
            'prerelease': '-' in value}


def target():
    system = platform.system().lower()
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(platform.machine().lower())
    assert (system, arch) in TARGETS, 'unsupported native release target'
    return system, arch


def clean_env():
    env = os.environ.copy()
    for key in ['DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH', 'LD_LIBRARY_PATH',
                'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS',
                'GOFLAGS', 'GOOS', 'GOARCH']:
        env.pop(key, None)
    env.update(CGO_ENABLED='1', GOTOOLCHAIN='local', GOENV='off', GOTELEMETRY='off', GOAMD64='v1', GOARM64='v8.0')
    return env


def mac_dependencies(path):
    rows = run('otool', '-L', path).splitlines()[1:]
    return [line.strip().split(' (', 1)[0] for line in rows]


def system_mac(name):
    return name.startswith(('/usr/lib/', '/System/Library/'))


def sign(path):
    run('codesign', '--force', '--sign', '-', '--timestamp=none', path)
    run('codesign', '--verify', '--strict', path)


def relocate_mac(path, library=False):
    if library:
        run('install_name_tool', '-id', '@rpath/'+path.name, path)
    for dependency in mac_dependencies(path):
        name = Path(dependency).name
        if library and name == path.name:
            continue
        if system_mac(dependency):
            continue
        assert name in {'libssl.3.dylib', 'libcrypto.3.dylib'}, f'unbundled dependency: {dependency}'
        desired = '@loader_path/'+('' if library else 'lib/')+name
        if dependency != desired:
            run('install_name_tool', '-change', dependency, desired, path)
    sign(path)


def linux_dependencies(path, env):
    result = subprocess.run(['ldd', str(path)], env=env, text=True, capture_output=True)
    if result.returncode and 'not a dynamic executable' in result.stdout+result.stderr:
        return {}
    if result.returncode:
        raise RuntimeError(f'ldd failed for {path}:\n{result.stdout}{result.stderr}')
    assert 'not found' not in result.stdout, 'unresolved Linux runtime dependency'
    entries = {}
    for line in result.stdout.splitlines():
        match = re.match(r'\s*(\S+)\s+=>\s+(/\S+)\s+\(', line)
        if match:
            entries[match[1]] = Path(match[2])
    return entries


def relocate_linux(path, library, env):
    # Go executables such as the MCP bridge may need only glibc. Rewriting
    # their ELF layout adds no useful search path and can break Go binaries
    # with older patchelf versions supplied by the minimum supported distro.
    if linux_dependencies(path, env).keys() - SYSTEM_LINUX:
        run('patchelf', '--set-rpath', '$ORIGIN' if library else '$ORIGIN/lib', path)


def dependency_closure(root, system, env):
    """Refuse artifacts that still resolve a non-system library outside the bundle."""
    for path in root.iterdir():
        if path.name.startswith('wispcat') and path.is_file() and os.access(path, os.X_OK):
            if system == 'darwin':
                run('codesign', '--verify', '--strict', path)
                for dep in mac_dependencies(path):
                    assert system_mac(dep) or dep.startswith('@loader_path/lib/'), dep
            else:
                for name, resolved in linux_dependencies(path, env).items():
                    assert name in SYSTEM_LINUX or resolved.resolve().is_relative_to((root/'lib').resolve()), (name, resolved)
    for path in (root/'lib').iterdir():
        if system == 'darwin':
            for dep in mac_dependencies(path):
                assert system_mac(dep) or dep.startswith(('@loader_path/', '@rpath/')), dep
            run('codesign', '--verify', '--strict', path)
        else:
            for name, resolved in linux_dependencies(path, env).items():
                assert name in SYSTEM_LINUX or resolved.resolve().is_relative_to((root/'lib').resolve()), (name, resolved)


def make_archive(stage, destination, epoch):
    """Write only a closed, explicit staging tree; never archive the worktree."""
    with destination.open('wb') as output:
        with gzip.GzipFile(filename='', mode='wb', fileobj=output, mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode='w') as archive:
                for path in [stage, *sorted(stage.rglob('*'))]:
                    assert not path.is_symlink(), 'release bundle must not contain symlinks'
                    info = archive.gettarinfo(str(path), str(path.relative_to(stage.parent)))
                    info.uid = info.gid = 0
                    info.uname = info.gname = ''
                    info.mtime = epoch
                    info.mode = 0o755 if path.is_dir() or os.access(path, os.X_OK) else 0o644
                    if path.is_file():
                        with path.open('rb') as body:
                            archive.addfile(info, body)
                    else:
                        archive.addfile(info)


def archive_name(tag, system, arch):
    version(tag)
    assert (system, arch) in TARGETS
    return f'wispcat_{tag}_{system}_{arch}.tar.gz'


def build(args):
    info = source(args.tag)
    system, arch = target()
    assert (system, arch) == (args.os, args.arch), 'runner architecture does not match the release label'
    env = clean_env()
    env.update(GOOS=system, GOARCH=arch)
    prefix = None
    if system == 'darwin':
        prefix = Path(run('brew', '--prefix', 'openssl@3'))
        env.update(CGO_CFLAGS=f'-I{prefix}/include', CGO_LDFLAGS=f'-L{prefix}/lib', MACOSX_DEPLOYMENT_TARGET='15.0')
    else:
        assert platform.libc_ver()[0] == 'glibc' and platform.libc_ver()[1] == '2.35', 'Linux release builders must use glibc 2.35 (Ubuntu 22.04)'
        env.pop('PKG_CONFIG_PATH', None)
        triplet = run('gcc', '-print-multiarch')
        env['PKG_CONFIG_LIBDIR'] = f'/usr/lib/{triplet}/pkgconfig:/usr/share/pkgconfig'
    work = ROOT/'.bin/release-work'
    work.mkdir(parents=True, exist_ok=True)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    names = ['wispcat', 'wispcat-mcp'] + (['wispcat-credentials'] if system == 'darwin' else [])
    with tempfile.TemporaryDirectory(prefix='bundle-', dir=work) as temporary:
        stage = Path(temporary)/f'wispcat_{args.tag}_{system}_{arch}'
        stage.mkdir()
        (stage/'lib').mkdir()
        (stage/'licenses').mkdir()
        for name in names:
            run('go', 'build', '-trimpath', '-ldflags=-s -w', '-o', stage/name, './cmd/'+name, env=env, capture=False)
        probe = work/f'native-{system}-{arch}.test'
        run('go', 'test', '-c', '-trimpath', '-o', probe, './internal/native', env=env, capture=False)
        if system == 'darwin':
            for name in ['libssl.3.dylib', 'libcrypto.3.dylib']:
                path = stage/'lib'/name
                shutil.copy2(prefix/'lib'/name, path)
                path.chmod(0o755)
                relocate_mac(path, True)
            for name in names:
                relocate_mac(stage/name)
            shutil.copy2(prefix/'LICENSE.txt', stage/'licenses/OPENSSL-LICENSE.txt')
            openssl_version = run(prefix/'bin/openssl', 'version')
        else:
            dependencies = {}
            for name in names:
                dependencies.update(linux_dependencies(stage/name, env))
            assert BUNDLED_LINUX <= dependencies.keys(), 'required OpenSSL/SQLite runtime libraries are missing'
            for name, path in dependencies.items():
                if name in SYSTEM_LINUX:
                    continue
                assert name in BUNDLED_LINUX, f'unexpected dependency requires explicit packaging/license review: {name}'
                shutil.copy2(path, stage/'lib'/name)
            for path in (stage/'lib').iterdir():
                relocate_linux(path, True, env)
            for name in names:
                relocate_linux(stage/name, False, env)
            for source_path, name in [('/usr/share/doc/libssl-dev/copyright', 'OPENSSL-NOTICES.txt'),
                                      ('/usr/share/common-licenses/Apache-2.0', 'OPENSSL-LICENSE.txt'),
                                      ('/usr/share/doc/libsqlite3-dev/copyright', 'SQLITE-NOTICES.txt')]:
                shutil.copy2(source_path, stage/'licenses'/name)
            openssl_version = run('/usr/bin/openssl', 'version')
        dependency_closure(stage, system, env)
        for name in ['LICENSE', 'THIRD_PARTY_NOTICES', 'wispcat.example.json']:
            shutil.copy2(ROOT/name, stage/name)
        shutil.copy2(ROOT/'internal/host/web/dist/THIRD_PARTY_NOTICES.txt', stage/'licenses/FRONTEND-NOTICES.txt')
        info.update(os=system, arch=arch, go_version=run('go', 'version'), openssl=openssl_version,
                    executables=names, minimum_runtime='macOS 15+' if system == 'darwin' else 'Linux with glibc 2.35+',
                    signing='ad-hoc, not notarized' if system == 'darwin' else 'SHA256SUMS')
        (stage/'BUILD_INFO.json').write_text(json.dumps(info, ensure_ascii=False, indent=2)+'\n')
        (stage/'README.md').write_text(bundle_readme(info))
        destination = out/archive_name(args.tag, system, arch)
        make_archive(stage, destination, int(run('git', 'show', '-s', '--format=%ct', 'HEAD')))
    print(json.dumps({'archive': str(destination.relative_to(ROOT)), 'sha256': digest(destination), 'target': f'{system}/{arch}'}))


def bundle_readme(info):
    return f'''# WispCat {info['version']}

Target: {info['os']}/{info['arch']} · {info['minimum_runtime']}

This bundle includes the executables, embedded management UI and required
OpenSSL runtime libraries. Linux bundles also include SQLite; macOS uses the
SQLite library supplied by the operating system. No Go or Node.js installation
is needed to run it. Keep the executables and lib/ directory together.

```sh
cp wispcat.example.json wispcat.local.json
export WISPCAT_ADMIN_TOKEN="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \\n')"
export WISPCAT_ONEBOT_TOKEN="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \\n')"
export WISPCAT_BOT_TOKEN="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \\n')"
./wispcat check --config wispcat.local.json
printf 'Management token: %s\\n' "$WISPCAT_ADMIN_TOKEN"
./wispcat serve --config wispcat.local.json
```

Open http://127.0.0.1:5099/ and enter the management token. The example account
starts offline. Save tokens in a private startup environment and configure the
bot framework with matching tokens. State and credential files are sensitive.
Mac credential extraction requires the validated WeChat 4.1.13 installation.
Linux imports an encrypted .wxlogin file through the management page.

macOS binaries use ad-hoc signing and have not been notarized by Apple.
This is an Alpha development preview. Read LICENSE, THIRD_PARTY_NOTICES and
https://github.com/JFSAS/WispCat for supported features, setup and usage limits.
'''


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def extract(archive, directory):
    with tarfile.open(archive, 'r:gz') as file:
        members = file.getmembers()
        assert members and len({Path(x.name).parts[0] for x in members}) == 1
        for member in members:
            path = Path(member.name)
            assert not path.is_absolute() and '..' not in path.parts
            assert member.isdir() or member.isfile(), 'links and special files are not release artifacts'
        file.extractall(directory, filter='data')
        return directory/Path(members[0].name).parts[0]


def isolated(command, env, capture=False, input=None, timeout=600):
    if platform.system() == 'Darwin':
        profile = '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))'
        return run('sandbox-exec', '-p', profile, *command, env=env, capture=capture, input=input, timeout=timeout)
    run('sh', ROOT/'tools/require-loopback-network.sh')
    return run(*command, env=env, capture=capture, input=input, timeout=timeout)


def verify(args):
    system, arch = target()
    assert args.archive.name == archive_name(args.tag, system, arch)
    env = clean_env()
    with tempfile.TemporaryDirectory(prefix='wispcat-unpack-') as temporary:
        root = extract(args.archive, Path(temporary))
        info = json.loads((root/'BUILD_INFO.json').read_text())
        assert info['tag'] == args.tag and info['os'] == system and info['arch'] == arch
        assert info['commit'] == source(args.tag)['commit']
        dependency_closure(root, system, env)
        probe = root/'.native-check'
        shutil.copy2(ROOT/f'.bin/release-work/native-{system}-{arch}.test', probe)
        if system == 'darwin':
            relocate_mac(probe)
        else:
            relocate_linux(probe, False, env)
        isolated([probe, '-test.v'], env)
        # The actual uncompressed executable starts under a fresh synthetic HOME;
        # no existing credentials or state are read by the process smoke probe.
        isolated([sys.executable, ROOT/'tools/offline-process-smoke.py', '--binary', root/'wispcat', '--report', '/dev/null'], env)
        request = {'jsonrpc':'2.0', 'id':1, 'method':'initialize', 'params':{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'release-probe','version':'1'}}}
        mcp_env = env.copy()
        for key in list(mcp_env):
            if key.startswith(('WISPCAT_MCP_', 'WECHAT_MCP_')):
                mcp_env.pop(key)
        result = isolated([root/'wispcat-mcp'], mcp_env, capture=True, input=json.dumps(request)+'\n', timeout=10)
        response = json.loads(result)
        assert response['result']['serverInfo']['version'] == version(args.tag)
        probe.unlink()
    print(json.dumps({'bundle':args.archive.name, 'native_transport_tests':'pass', 'offline_startup':'pass', 'mcp_initialize':'pass', 'live_wechat_calls':0}))


def publish(args):
    info = source(args.tag)
    repository = os.environ['GH_REPO']
    expected = {archive_name(args.tag, system, arch) for system, arch in TARGETS}
    archives = {p.name:p for p in args.directory.glob('*.tar.gz')}
    assert archives.keys() == expected, 'release requires all four verified target archives'
    for name, path in archives.items():
        with tempfile.TemporaryDirectory(prefix='wispcat-publish-check-') as temporary:
            root = extract(path, Path(temporary))
            metadata = json.loads((root/'BUILD_INFO.json').read_text())
            assert metadata['tag'] == args.tag and metadata['commit'] == info['commit']
            assert name == archive_name(args.tag, metadata['os'], metadata['arch'])
    refs = dict(line.split()[::-1] for line in run('git','ls-remote','origin',f'refs/tags/{args.tag}',f'refs/tags/{args.tag}^{{}}').splitlines())
    assert refs.get(f'refs/tags/{args.tag}^{{}}',refs.get(f'refs/tags/{args.tag}')) == info['commit'], 'tag moved after build'
    deadline = time.monotonic()+300
    while True:
        runs = json.loads(run('gh','run','list','--repo',repository,'--workflow','alpha.yml','--commit',info['commit'],'--json','status,conclusion','--limit','10'))
        if any(x['status']=='completed' and x['conclusion']=='success' for x in runs):
            break
        if not runs or all(x['status']=='completed' for x in runs) or time.monotonic()>deadline:
            raise RuntimeError('Alpha checks for the exact release commit must pass before publication')
        print('Waiting for Alpha checks for the release commit.', flush=True)
        time.sleep(10)
    checksums = args.directory/'SHA256SUMS'
    checksums.write_text(''.join(f'{digest(archives[name])}  {name}\n' for name in sorted(expected)))
    notes = args.directory/'release-notes.md'
    notes.write_text(f'''WispCat {info['version']} · Alpha 开发预览。\n\n本版提供 macOS Apple Silicon／Intel 与 Linux x64／ARM64 四个可执行发行包，前端已嵌入程序，包含 OpenSSL 运行库、许可文件和构建信息；Linux 包另含 SQLite。\n\n下载匹配架构的 `.tar.gz`，解压后按包内 README 启动，并使用 `SHA256SUMS` 验证下载。macOS 包要求 macOS 15+，采用 ad-hoc 签名，尚未 Apple 公证；Linux 包要求 glibc 2.35+。Windows 暂未发布。\n\n所有发行包均在对应架构原生构建，并在解压后的独立目录检查动态库依赖、OpenSSL 传输与证书验证、合成离线账号启动及 MCP 初始化。验证不登录真实微信或调用模型。\n\n协议或客户端变化仍可能影响连接与账号安全；支持范围、凭证登录步骤、源码可见授权限制及已知未验收项见 [README](https://github.com/{repository}/blob/{args.tag}/README.md)。\n\n源代码提交：`{info['commit']}`。\n''')
    existing = subprocess.run(['gh','release','view',args.tag,'--repo',repository,'--json','isDraft,url'],text=True,capture_output=True)
    assets = [checksums, *[archives[name] for name in sorted(expected)]]
    if existing.returncode == 0 and not json.loads(existing.stdout)['isDraft']:
        with tempfile.TemporaryDirectory(prefix='wispcat-existing-assets-') as temporary:
            run('gh','release','download',args.tag,'--repo',repository,'--dir',temporary)
            assert all((Path(temporary)/p.name).is_file() and digest(Path(temporary)/p.name) == digest(p) for p in assets), 'published assets differ; create a new release tag instead of replacing them'
        print('Published release already has the identical complete asset set.')
        return
    if existing.returncode:
        run('gh','release','create',args.tag,'--repo',repository,'--verify-tag','--draft',
            '--title',f'WispCat {args.tag} · Alpha 开发预览','--notes-file',notes)
    run('gh','release','upload',args.tag,'--repo',repository,'--clobber',*assets)
    run('gh','release','edit',args.tag,'--repo',repository,'--draft=false',
        '--prerelease='+str(info['prerelease']).lower(),'--latest=false','--notes-file',notes)
    print(run('gh','release','view',args.tag,'--repo',repository,'--json','url'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    metadata = commands.add_parser('metadata')
    metadata.add_argument('--tag', required=True)
    metadata.add_argument('--allow-untagged', action='store_true', help='build-only workflow preview; publishing still requires the exact remote tag')
    builder = commands.add_parser('build')
    builder.add_argument('--tag', required=True)
    builder.add_argument('--os', choices=['darwin','linux'], required=True)
    builder.add_argument('--arch', choices=['arm64','amd64'], required=True)
    builder.add_argument('--output', type=Path, default=ROOT/'.bin/release')
    checker = commands.add_parser('verify')
    checker.add_argument('--tag', required=True)
    checker.add_argument('--archive', type=Path, required=True)
    publisher = commands.add_parser('publish')
    publisher.add_argument('--tag', required=True)
    publisher.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'metadata':
        info = source(args.tag)
        if not args.allow_untagged:
            assert run('git','rev-parse',f'refs/tags/{args.tag}^{{commit}}') == info['commit'], 'checkout must match the exact tag'
        with open(os.environ['GITHUB_OUTPUT'],'a') as output:
            for key,value in info.items():
                output.write(f'{key}={str(value).lower() if isinstance(value,bool) else value}\n')
    elif args.command == 'build':
        build(args)
    elif args.command == 'verify':
        verify(args)
    else:
        publish(args)


if __name__ == '__main__':
    main()
