"""Release archive regressions; synthetic files only, no native builds or network."""
import importlib.util
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('release', Path(__file__).with_name('release.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def test_linux_relocation_preserves_system_only_go_executables(self):
        with patch.object(release, 'linux_dependencies') as dependencies, patch.object(release, 'run') as run:
            for resolved in [{}, {'libc.so.6': Path('/lib/libc.so.6')}]:
                dependencies.return_value = resolved
                release.relocate_linux(Path('wispcat-mcp'), False, {})
                run.assert_not_called()
            dependencies.return_value = {'libssl.so.3': Path('/lib/libssl.so.3')}
            release.relocate_linux(Path('wispcat'), False, {})
            run.assert_called_once_with('patchelf', '--set-rpath', '$ORIGIN/lib', Path('wispcat'))
            run.reset_mock()
            release.relocate_linux(Path('libssl.so.3'), True, {})
            run.assert_called_once_with('patchelf', '--set-rpath', '$ORIGIN', Path('libssl.so.3'))

    def test_tag_and_architecture_validation(self):
        self.assertEqual(len(release.TARGETS), 4)
        for bad in ['main', 'v1.0.0;echo unsafe', 'v1.0.0\n', '../v1.0.0']:
            with self.assertRaises(ValueError):
                release.version(bad)
        self.assertEqual(release.archive_name('v1.2.3-alpha.1', 'linux', 'arm64'), 'wispcat_v1.2.3-alpha.1_linux_arm64.tar.gz')
        with self.assertRaises(AssertionError):
            release.archive_name('v1.2.3', 'windows', 'amd64')

    def test_archive_is_reproducible_and_contains_only_staged_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stage = root/'bundle'
            stage.mkdir()
            (stage/'wispcat').write_text('synthetic executable')
            (stage/'wispcat').chmod(0o755)
            (root/'private.env').write_text('never include sibling files')
            release.make_archive(stage, root/'a.tar.gz', 1234)
            release.make_archive(stage, root/'b.tar.gz', 1234)
            self.assertEqual(release.digest(root/'a.tar.gz'), release.digest(root/'b.tar.gz'))
            with tarfile.open(root/'a.tar.gz') as archive:
                self.assertEqual(archive.getnames(), ['bundle', 'bundle/wispcat'])
                self.assertEqual(archive.getmember('bundle/wispcat').mode, 0o755)
            out = root/'unpack'
            out.mkdir()
            self.assertEqual((release.extract(root/'a.tar.gz', out)/'wispcat').read_text(), 'synthetic executable')

    def test_extract_rejects_traversal_links_and_multiple_roots(self):
        cases = [('bundle/../../escape', tarfile.REGTYPE), ('/absolute', tarfile.REGTYPE),
                 ('bundle/link', tarfile.SYMTYPE), ('bundle/hardlink', tarfile.LNKTYPE),
                 ('bundle/fifo', tarfile.FIFOTYPE)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, kind in cases:
                with self.subTest(name=name):
                    with tarfile.open(root/'bad.tar.gz', 'w:gz') as archive:
                        entry = tarfile.TarInfo(name)
                        entry.type, entry.linkname = kind, '/tmp/escape'
                        archive.addfile(entry)
                    with self.assertRaises(AssertionError):
                        release.extract(root/'bad.tar.gz', root)
            with tarfile.open(root/'bad.tar.gz', 'w:gz') as archive:
                for name in ['one/file', 'two/file']:
                    archive.addfile(tarfile.TarInfo(name))
            with self.assertRaises(AssertionError):
                release.extract(root/'bad.tar.gz', root)


if __name__ == '__main__':
    unittest.main()
