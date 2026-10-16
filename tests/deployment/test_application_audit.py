"""Pure audit-argument negatives: no child process or AstrBot import."""
import os
from pathlib import Path
import tempfile
import unittest

from application_audit import is_noop_sandbox_probe


class ProbeArguments(unittest.TestCase):
    def test_exact_command_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve()
            workspace=root/'data/temp/sandbox-probe-synthetic';workspace.mkdir(parents=True)
            command=['/usr/bin/sandbox-exec','-D',f'WORKSPACE={workspace}',
                '-p','(version 1)(deny network*)','/usr/bin/env','-i',
                '/fixture/python','-I','-S','-c','resource limit wrapper','/bin/sh','-c',':']
            valid=[command[0],command,str(workspace),{'PATH':os.defpath}]
            expected=lambda _:command
            self.assertTrue(is_noop_sandbox_probe(valid,root,expected))
            self.assertTrue(is_noop_sandbox_probe([valid[0],tuple(command),workspace,valid[3]],root,expected))
            changes={
                'actual executable override':(0,'/bin/sh'),
                'same basename executable':(0,'/tmp/sandbox-exec'),
                'foreign working directory':(2,str(root)),
                'extra environment':(3,{'PATH':os.defpath,'INJECT':'1'}),
            }
            for label,(index,value) in changes.items():
                with self.subTest(label=label):
                    args=list(valid);args[index]=value
                    self.assertFalse(is_noop_sandbox_probe(args,root,expected))
            mutations={
                'same basename argv':(0,'/tmp/sandbox-exec'),
                'network allowed policy':(4,'(version 1)(allow network*)'),
                'wrapper code replacement':(11,'other code'),
                'non-noop shell':(-1,'echo changed'),
                'workspace outside fixture':(2,f'WORKSPACE={root}'),
            }
            for label,(index,value) in mutations.items():
                with self.subTest(label=label):
                    argv=list(command);argv[index]=value
                    self.assertFalse(is_noop_sandbox_probe([valid[0],argv,*valid[2:]],root,expected))
            with self.subTest(label='extra argument'):
                self.assertFalse(is_noop_sandbox_probe([valid[0],command+['extra'],*valid[2:]],root,expected))


if __name__=='__main__':unittest.main()
