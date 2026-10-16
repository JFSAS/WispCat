"""Join a fixture Host before deleting its private state, including failures."""
import shutil
import subprocess


def close_host(host, log, reserve, directory, terminate_timeout=10, kill_timeout=3):
    try:
        if host is not None:
            if host.poll() is None:
                host.terminate()
            try:
                host.wait(timeout=terminate_timeout)
            except subprocess.TimeoutExpired:
                host.kill()
                host.wait(timeout=kill_timeout)
    finally:
        try:
            if log is not None:
                log.close()
        finally:
            try:
                reserve.close()
            finally:
                # A failed join must preserve the state. The directory has no
                # automatic finalizer that could delete it under a live Host.
                if host is None or host.poll() is not None:
                    shutil.rmtree(directory)
