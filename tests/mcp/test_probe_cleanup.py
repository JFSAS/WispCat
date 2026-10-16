"""Bounded failure-path ownership, independent of the MCP SDK."""
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

from probe_cleanup import close_host


class HostCleanup(unittest.TestCase):
    def test_sigterm_ignored_child_is_killed_and_joined_before_state_removal(self):
        directory=Path(tempfile.mkdtemp())
        log=(directory/'host.log').open('w+')
        reserve=socket.socket()
        child=subprocess.Popen([sys.executable,'-c',
            'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print("ready",flush=True); time.sleep(60)'],
            stdout=subprocess.PIPE,text=True)
        try:
            self.assertEqual(child.stdout.readline(),'ready\n')
            close_host(child,log,reserve,directory,terminate_timeout=.1)
            self.assertEqual(child.returncode,-signal.SIGKILL)
            self.assertTrue(log.closed)
            self.assertEqual(reserve.fileno(),-1)
            self.assertFalse(directory.exists())
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=3)
            child.stdout.close();log.close();reserve.close()
            shutil.rmtree(directory,ignore_errors=True)

    def test_failed_join_preserves_state_and_closes_handles(self):
        directory=Path(tempfile.mkdtemp())
        log=(directory/'host.log').open('w+');reserve=socket.socket()
        host=Mock();host.poll.return_value=None
        host.wait.side_effect=subprocess.TimeoutExpired('synthetic-host',.1)
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                close_host(host,log,reserve,directory,terminate_timeout=.1,kill_timeout=.1)
            host.terminate.assert_called_once();host.kill.assert_called_once()
            self.assertTrue(directory.is_dir());self.assertTrue(log.closed)
            self.assertEqual(reserve.fileno(),-1)
        finally:shutil.rmtree(directory)


if __name__=='__main__':unittest.main()
