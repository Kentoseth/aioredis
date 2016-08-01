from .parser import (
    parse_sentinel_masters,
    parse_sentinel_get_master,
    parse_sentinel_master,
    parse_sentinel_slaves_and_sentinels,
    )


class RedisSentinel:
    """Redis sentinel client."""

    def __init__(self, pool):
        self._pool = pool

    def get_master(self, name):
        """Returns Redis client to master Redis server."""

    def get_slave(self, name):
        """Returns Redis client to slave Redis server."""

    def execute(self, command, *args, **kwargs):
        return self._pool.execute(b'SENTINEL', command, *args, **kwargs)

    def ping(self):
        return (yield from self._pool.execute(b'PING'))
        # return (yield from self.execute(b'PING'))

    def master(self, name):
        """Returns a dictionary containing the specified masters state."""
        fut = self.execute(b'MASTER', name, encoding='utf-8')
        return parse_sentinel_master(fut)

    def master_address(self, name):
        """Returns a (host, port) pair for the given ``name``."""
        fut = self.execute(b'get-master-addr-by-name', name, encoding='utf-8')
        return parse_sentinel_get_master(fut)

    def masters(self):
        """Returns a list of dictionaries containing each master's state."""
        masters = self.execute(b'MASTERS', encoding='utf-8')
        # TODO: process masters
        return parse_sentinel_masters(masters)

    def slaves(self, name):
        """Returns a list of slaves for ``name``"""
        fut = self.execute(b'SLAVES', name, encoding='utf-8')
        return parse_sentinel_slaves_and_sentinels(fut)

    def sentinels(self, name):
        """Returns a list of sentinels for ``name``"""
        fut = self.execute(b'SENTINELS', name, encoding='utf-8')
        return parse_sentinel_slaves_and_sentinels(fut)

    def monitor(self, name, ip, port, quorum):
        """Add a new master to Sentinel to be monitored"""

    def remove(self, name):
        """Remove a master from Sentinel's monitoring"""

    def set(self, name, option, value):
        """Set Sentinel monitoring parameters for a given master"""

    def failover(self, name):
        """Force a failover of a named master."""

    def check_quorum(self, name):
        """ """
