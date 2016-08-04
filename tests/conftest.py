import asyncio
import pytest
import socket
import subprocess
import sys
import re
import contextlib
import os
import ssl
import time
import logging

from collections import namedtuple

import aioredis
import aioredis.sentinel2


TCPAddress = namedtuple('TCPAddress', 'host port')

RedisServer = namedtuple('RedisServer', 'name tcp_address unixsocket version')

SentinelServer = namedtuple('SentinelServer',
                            'name tcp_address unixsocket version masters')

# Public fixtures


@pytest.yield_fixture
def loop():
    """Creates new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if hasattr(loop, 'is_closed'):
        closed = loop.is_closed()
    else:
        closed = loop._closed   # XXX
    if not closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()


@pytest.fixture(scope='session')
def unused_port():
    """Gets random free port."""
    def fun():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return fun


@pytest.fixture
def create_connection(_closable, loop):
    """Wrapper around aioredis.create_connection."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        conn = yield from aioredis.create_connection(*args, **kw)
        _closable(conn)
        return conn
    return f


@pytest.fixture(params=['single', 'pool'])
def create_redis(_closable, loop, request):
    """Wrapper around aioredis.create_redis."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        if request.param == 'single':
            redis = yield from aioredis.create_redis(*args, **kw)
        else:
            redis = yield from aioredis.create_redis_pool(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def create_pool(_closable, loop):
    """Wrapper around aioredis.create_pool."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        redis = yield from aioredis.create_pool(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def create_sentinel(create_pool, loop):
    """Helper instantiating RedisSentinel client."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        pool = yield from create_pool(*args, **kw)
        return aioredis.sentinel2.RedisSentinel(pool)
    return f


@pytest.fixture
def pool(create_pool, server, loop):
    """Returns RedisPool instance."""
    pool = loop.run_until_complete(
        create_pool(server.tcp_address, loop=loop))
    return pool


@pytest.fixture
def redis(create_redis, server, loop):
    """Returns Redis client instance."""
    redis = loop.run_until_complete(
        create_redis(server.tcp_address, loop=loop))
    loop.run_until_complete(redis.flushall())
    return redis


@pytest.yield_fixture
def _closable(loop):
    conns = []

    yield conns.append

    waiters = []
    while conns:
        conn = conns.pop(0)
        conn.close()
        waiters.append(conn.wait_closed())
    if waiters:
        loop.run_until_complete(asyncio.gather(*waiters, loop=loop))


@pytest.fixture(scope='session')
def server(start_server):
    """Starts redis-server instance."""
    return start_server('A')


@pytest.fixture(scope='session')
def serverB(start_server):
    """Starts redis-server instance."""
    return start_server('B')


@pytest.fixture(scope='session')
def sentinel(start_sentinel, request, start_server):
    """Starts redis-sentinel instance with one master -- masterA."""
    masterA = start_server('masterA', [
        'slave-read-only yes',
        ])
    start_server('slaveA', [
        'slaveof 127.0.0.1 {}'.format(masterA.tcp_address.port),
        'slave-read-only yes',
        ])
    time.sleep(3)
    return start_sentinel('main', masterA)


# Internal stuff #


def pytest_addoption(parser):
    parser.addoption('--redis-server', default='/usr/bin/redis-server',
                     help="Path to redis-server executable,"
                          " defaults to `%(default)s`")
    parser.addoption('--ssl-cafile', default='tests/ssl/test.crt',
                     help="Path to testing SSL certificate")
    parser.addoption('--uvloop', default=False,
                     action='store_true',
                     help="Run tests with uvloop")


@pytest.mark.trylast
def pytest_terminal_summary(terminalreporter):
    if os.environ.get('TRAVIS'):
        if bool(terminalreporter.config.getoption('--cov', False)):
            print("\ntravis_fold:end:coverage")


def _headline(title):
    return "\033[33;1m{}{:=<15}{}\033[0m".format(
        "=" * 20, " {} ".format(title), "=" * 20)


def pytest_sessionstart(session):
    if os.environ.get('TRAVIS'):
        print("\ntravis_fold:start:tests")
        print(_headline("Tests"))


def pytest_sessionfinish(session, exitstatus):
    if os.environ.get('TRAVIS'):
        print("\ntravis_fold:end:tests")
        if bool(session.config.getoption('--cov', False)):
            print("travis_fold:start:coverage")
            print(_headline("Coverage"))


def _read_server_version(config):
    args = [config.getoption('--redis-server'), '--version']
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode('utf-8')
    for part in version.split():
        if part.startswith('v='):
            break
    else:
        raise RuntimeError(
            "No version info can be found in {}".format(version))
    return tuple(map(int, part[2:].split('.')))


@contextlib.contextmanager
def config_writer(path):
    with open(path, 'wt') as f:
        def write(*args):
            print(*args, file=f)
        yield write


@pytest.fixture(scope='session')
def start_server(_proc, request, unused_port):
    """Starts Redis server instance.

    Caches instances by name.
    ``name`` param -- instance alias
    ``config_lines`` -- optional list of config directives to put in config
        (if no config_lines passed -- no config will be generated,
         for backward compatibility).
    """

    version = _read_server_version(request.config)
    verbose = request.config.getoption('-v') > 3

    servers = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(name, config_lines=None):
        if name in servers:
            return servers[name]

        port = unused_port()
        tcp_address = TCPAddress('localhost', port)
        unixsocket = '/tmp/aioredis.{}.sock'.format(port)
        if config_lines:
            config = '/tmp/aioreids.{}.conf'.format(port)
            with config_writer(config) as write:
                write('daemonize no')
                write('save ""')
                write('port', port)
                write('unixsocket', unixsocket)
                write('# extra config')
                for line in config_lines:
                    write(line)
            args = [config]
            tmp_files = [config]
        else:
            args = ['--daemonize', 'no',
                    '--save', '""',
                    '--port', str(port),
                    '--unixsocket', unixsocket,
                    ]
            tmp_files = []

        proc = _proc(request.config.getoption('--redis-server'),
                     *args,
                     stdout=subprocess.PIPE,
                     _clear_tmp_files=tmp_files)
        for _ in timeout(3):
            log = proc.stdout.readline()
            if log and verbose:
                print(log)
            if b'The server is now ready to accept connections ' in log:
                break

        info = RedisServer(name, tcp_address, unixsocket, version)
        servers.setdefault(name, info)
        return info

    return maker


@pytest.fixture(scope='session')
def start_sentinel(_proc, request, unused_port, start_server):
    """Starts Redis Sentinel instances."""
    version = _read_server_version(request.config)
    verbose = request.config.getoption('-v') > 3

    sentinels = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(name, *masters):
        key = (name,) + masters
        if key in sentinels:
            return sentinels[key]
        port = unused_port()
        tcp_address = TCPAddress('localhost', port)
        unixsocket = '/tmp/aioredis-sentinel.{}.sock'.format(port)
        config = '/tmp/aioredis-sentinel.{}.conf'.format(port)

        with config_writer(config) as write:
            write('daemonize no')
            write('save ""')
            write('port', port)
            write('unixsocket', unixsocket)
            for master in masters:
                write('sentinel monitor', master.name,
                      '127.0.0.1', master.tcp_address.port, '2')

        proc = _proc(request.config.getoption('--redis-server'),
                     config,
                     '--sentinel',
                     stdout=subprocess.PIPE,
                     _clear_tmp_files=[config])
        for _ in timeout(3):
            log = proc.stdout.readline()
            if log and verbose:
                print(log)
            if b'# +monitor master ' in log:
                break
        else:
            raise RuntimeError("Could not start Sentinel")

        masters = {m.name: m for m in masters}
        info = SentinelServer(name, tcp_address, unixsocket, version, masters)
        sentinels.setdefault(key, info)
        return info
    return maker


@pytest.fixture(scope='session')
def ssl_proxy(_proc, request, unused_port):
    by_port = {}

    cafile = os.path.abspath(request.config.getoption('--ssl-cafile'))
    pemfile = os.path.splitext(cafile)[0] + '.pem'
    assert os.path.exists(cafile), \
        "No test ssl certificate, run `make certificate`"
    assert os.path.exists(pemfile), \
        "No test ssl certificate, run `make certificate`"

    if hasattr(ssl, 'create_default_context'):
        ssl_ctx = ssl.create_default_context(cafile=cafile)
    else:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_ctx.load_verify_locations(cafile=cafile)
    if hasattr(ssl_ctx, 'check_hostname'):
        # available since python 3.4
        ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    def sockat(unsecure_port):
        if unsecure_port in by_port:
            return by_port[unsecure_port]

        secure_port = unused_port()
        _proc('/usr/bin/socat',
              'openssl-listen:{},'
              'cert={},verify=0,fork'.format(secure_port, pemfile),
              'tcp-connect:localhost:{}'.format(unsecure_port)
              )
        time.sleep(1)   # XXX
        by_port[unsecure_port] = secure_port, ssl_ctx
        return secure_port, ssl_ctx

    return sockat


@pytest.yield_fixture(scope='session')
def _proc():
    processes = []
    tmp_files = set()

    def run(*commandline, _clear_tmp_files=(), **kwargs):
        proc = subprocess.Popen(commandline, **kwargs)
        processes.append(proc)
        tmp_files.update(_clear_tmp_files)
        return proc

    yield run

    while processes:
        proc = processes.pop(0)
        proc.terminate()
        proc.wait()
    for path in tmp_files:
        os.remove(path)


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            # TODO: re-wrap with asyncio.coroutine if not native coroutine
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(
            asyncio.wait_for(pyfuncitem.obj(**testargs),
                             15, loop=loop))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_ignore_collect(path, config):
    if 'py35' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


def pytest_collection_modifyitems(session, config, items):
    version = _read_server_version(config)
    for item in items:
        if 'redis_version' in item.keywords:
            marker = item.keywords['redis_version']
            if version < marker.kwargs['version']:
                item.add_marker(pytest.mark.skip(
                    reason=marker.kwargs['reason']))
        if 'ssl_proxy' in item.fixturenames:
            item.add_marker(pytest.mark.skipif(
                "not os.path.exists('/usr/bin/socat')",
                reason="socat package required (apt-get install socat)"))


def pytest_configure(config):
    if config.getoption('--uvloop'):
        try:
            import uvloop
        except ImportError:
            raise RuntimeError(
                "Can not import uvloop, make sure it is installed")
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@contextlib.contextmanager
def raises_regex(exc_type, message):
    with pytest.raises(exc_type) as exc_info:
        yield exc_info
    match = re.search(message, str(exc_info.value))
    assert match is not None, (
        "Pattern {!r} does not match {!r}"
        .format(message, str(exc_info.value)))


def logs(logger, level=None):
    """Catches logs for given logger and level.

    See unittest.TestCase.assertLogs for details.
    """
    return _AssertLogsContext(logger, level)


_LoggingWatcher = namedtuple("_LoggingWatcher", ["records", "output"])


class _CapturingHandler(logging.Handler):
    """
    A logging handler capturing all (raw and formatted) logging output.
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.watcher = _LoggingWatcher([], [])

    def flush(self):
        pass

    def emit(self, record):
        self.watcher.records.append(record)
        msg = self.format(record)
        self.watcher.output.append(msg)


class _AssertLogsContext:
    """Standard unittest's _AssertLogsContext context manager
    adopted to raise pytest failure.
    """
    LOGGING_FORMAT = "%(levelname)s:%(name)s:%(message)s"

    def __init__(self, logger_name, level):
        self.logger_name = logger_name
        if level:
            self.level = level
        else:
            self.level = logging.INFO
        self.msg = None

    def __enter__(self):
        if isinstance(self.logger_name, logging.Logger):
            logger = self.logger = self.logger_name
        else:
            logger = self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(self.LOGGING_FORMAT)
        handler = _CapturingHandler()
        handler.setFormatter(formatter)
        self.watcher = handler.watcher
        self.old_handlers = logger.handlers[:]
        self.old_level = logger.level
        self.old_propagate = logger.propagate
        logger.handlers = [handler]
        logger.setLevel(self.level)
        logger.propagate = False
        return handler.watcher

    def __exit__(self, exc_type, exc_value, tb):
        self.logger.handlers = self.old_handlers
        self.logger.propagate = self.old_propagate
        self.logger.setLevel(self.old_level)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return False
        if len(self.watcher.records) == 0:
            pytest.fail(
                "no logs of level {} or higher triggered on {}"
                .format(logging.getLevelName(self.level), self.logger.name))


def redis_version(*version, reason):
    assert 1 < len(version) <= 3, version
    assert all(isinstance(v, int) for v in version), version
    return pytest.mark.redis_version(version=version, reason=reason)


def assert_almost_equal(first, second, places=None, msg=None, delta=None):
    assert not (places is None and delta is None)
    if delta is not None:
        assert abs(first - second) <= delta
    else:
        assert round(abs(first - second), places) == 0


def pytest_namespace():
    return {
        'raises_regex': raises_regex,
        'assert_almost_equal': assert_almost_equal,
        'redis_version': redis_version,
        'logs': logs,
        }
