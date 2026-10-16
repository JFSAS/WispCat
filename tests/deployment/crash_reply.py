"""Explicit fixture-only command, returning a numeric-loopback source URL."""
import ipaddress
import os
import urllib.parse


def source_for(text):
    if text != '/crash-transfer':
        return None
    source = os.environ.get('DEPLOYMENT_CRASH_SOURCE')
    if not source:
        return None
    value = urllib.parse.urlsplit(source)
    assert value.scheme == 'http' and value.hostname and ipaddress.ip_address(value.hostname).is_loopback
    assert value.port and not value.username and not value.password and not value.query and not value.fragment
    assert value.path == '/crash-source'
    return source
