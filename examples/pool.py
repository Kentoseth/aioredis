import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis_pool(
            ('localhost', 6379),
            minsize=5, maxsize=10)
        # with (yield from pool) as redis:    # high-level redis API instance
        yield from redis.set('my-key', 'value')
        val = yield from redis.get('my-key')
        print('raw value:', val)
        redis.close()
        yield from redis.wait_closed()    # closing all open connections

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
