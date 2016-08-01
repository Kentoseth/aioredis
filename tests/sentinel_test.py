import pytest
import asyncio

from unittest import mock

from aioredis import (
    ReadOnlyError,
    ReplyError,
    )


pytestmark = pytest.redis_version(2, 8, 0, reason="Sentinel v2 required")

get_master_connection = mock.Mock()
get_slave_connection = mock.Mock()
master_name = 'mymaster'
redis_sentinel = mock.Mock()


@pytest.mark.run_loop
def test_sentinel_simple(sentinel, create_redis, loop):
    redis = yield from create_redis(sentinel.tcp_address, loop=loop)
    info = yield from redis.role()
    assert info.role == 'sentinel'


@pytest.mark.run_loop
def test_sentinel_masters(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel(sentinel.tcp_address)

    res = yield from redis_sentinel.masters()
    assert res == {
        'masterA': {
            'config-epoch': 0,
            'down-after-milliseconds': 30000,
            'failover-timeout': 180000,
            'flags': 'master',
            'info-refresh': mock.ANY,
            'ip': '127.0.0.1',
            'is_disconnected': False,       # XXX: mixing "_" and "-"
            'is_master': True,
            'is_master_down': False,
            'is_odown': False,
            'is_sdown': False,
            'is_sentinel': False,
            'is_slave': False,              # make it enum?
            'last-ok-ping-reply': mock.ANY,
            'last-ping-reply': mock.ANY,
            'last-ping-sent': mock.ANY,
            'name': 'masterA',
            'num-other-sentinels': 0,
            'num-slaves': 1,
            'parallel-syncs': 1,
            'pending-commands': 0,
            'port': sentinel.masters['masterA'].tcp_address.port,
            'quorum': 2,
            'role-reported': 'master',
            'role-reported-time': mock.ANY,
            'runid': mock.ANY,
            }
        }


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_normal():
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_master_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret != 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from redis.hset(key, field, value)
    assert ret == 0


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_slave():
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_slave_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop       # (timeout=600)
def test_sentinel_slave_fail(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_slave_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    redis = yield from get_slave_connection()
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_normal_fail(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_master_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret == 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    ret = yield from redis.hset(key, field, value)
    assert ret == 0
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    redis = yield from get_slave_connection()
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_failover(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    func = sentinel_connection.sentinel_get_master_addr_by_name
    orig_master = yield from func(master_name)
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    new_master = yield from func(master_name)
    assert orig_master != new_master
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    new_master = yield from func(master_name)
    assert orig_master == new_master
    redis = yield from get_slave_connection()
    key, field, value = b'key:hset', b'bar', b'zap'
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except ReadOnlyError:
            break


@pytest.mark.run_loop
def test_get_master(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel(sentinel.tcp_address)
    # sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    master = yield from redis_sentinel.master_address('masterA')
    assert isinstance(master, tuple)
    assert len(master) == 2
    assert master[0] == '127.0.0.1'
    assert master[1] == sentinel.masters['masterA'].tcp_address.port


@pytest.mark.run_loop
def test_get_master_info(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel(sentinel.tcp_address)

    master = yield from redis_sentinel.master('masterA')
    assert isinstance(master, dict)
    assert master['is_slave'] is False
    assert master['name'] == 'masterA'
    for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
              'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
              'config-epoch', 'parallel-syncs', 'role-reported-time',
              'is_sentinel', 'last-ok-ping-reply',
              'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
              'name', 'pending-commands', 'down-after-milliseconds',
              'is_slave', 'num-slaves', 'port', 'is_disconnected',
              'role-reported']:
        assert k in master


@pytest.mark.run_loop
def test_get_slave_info(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel(sentinel.tcp_address)

    info = yield from redis_sentinel.slaves('masterA')
    assert len(info) == 1
    info = info[0]
    assert isinstance(info, dict)
    assert info['is_slave'] is True
    for k in ['is_master_down', 'flags', 'is_odown',
              'ip', 'runid', 'info-refresh',
              'role-reported-time',
              'is_sentinel', 'last-ok-ping-reply',
              'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
              'name', 'pending-commands', 'down-after-milliseconds',
              'is_slave', 'port', 'is_disconnected', 'role-reported']:
        assert k in info, k


@pytest.mark.run_loop
def test_get_sentinel_info(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel(sentinel.tcp_address)

    sentinel = yield from redis_sentinel.sentinels('masterA')
    assert len(sentinel) == 0

    with pytest.raises(ReplyError):
        yield from redis_sentinel.sentinels('bad_master')


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_set_error():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    with pytest.raises(ReplyError):
        yield from sentinel_connection.sentinel_set(master_name, 'foo', 'bar')


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_set():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    resp = yield from sentinel_connection.sentinel_set(
        master_name, 'failover-timeout', 1100)
    assert resp is True
    master = yield from sentinel_connection.sentinel_masters()
    assert master[master_name]['failover-timeout'] == 1100


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_monitor():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    master = yield from sentinel_connection.sentinel_masters()
    if len(master):
        if 'mymaster2' in master:
            resp = yield from sentinel_connection.sentinel_remove('mymaster2')
            assert resp is True
    resp = yield from sentinel_connection.sentinel_monitor('mymaster2',
                                                           '127.0.0.1',
                                                           6380, 2)
    assert resp is True
    resp = yield from sentinel_connection.sentinel_remove('mymaster2')
    assert resp is True
