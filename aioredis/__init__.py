from .connection import RedisConnection, create_connection
from .commands import Redis, create_redis, create_reconnecting_redis
from .commands import create_redis_pool
from .pool import ConnectionsPool, create_pool
from .util import Channel
from .sentinel import RedisSentinel, create_sentinel
from .errors import (
    ConnectionClosedError,
    MasterNotFoundError,
    MultiExecError,
    PipelineError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ReplyError,
    ChannelClosedError,
    WatchVariableError,
    PoolClosedError,
    SlaveNotFoundError,
    )


__version__ = '0.2.9'

RedisPool = ConnectionsPool

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_redis_pool, create_pool,
 create_sentinel, RedisSentinel,
 RedisPool, ConnectionsPool, Channel,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError, ConnectionClosedError,
 ChannelClosedError, WatchVariableError, PoolClosedError,
 MasterNotFoundError, SlaveNotFoundError, ReadOnlyError,
 )
