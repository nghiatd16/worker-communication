# -*- coding: utf-8 -*-

import threading
import time
import queue


class TooManyConnections(Exception):
    '''Khi có quá nhiều kết nối, exception này được ném ra'''


class Expired(Exception):
    '''Khi có kết nối không khả dụng, exception này được ném ra'''


class UsageExceeded(Expired):
    '''Số lần kết nối đã được sử dụng vượt quá giới hạn'''


class TtlExceeded(Expired):
    '''Thời gian sử dụng kết nối vượt quá vòng đời được chỉ định bởi ttl'''


class IdleExceeded(Expired):
    '''Thời gian không hoạt động vượt quá thời gian được chỉ định Idle time'''


class WrapperConnection(object):
    '''Được sử dụng để đóng gói các kết nối cơ sở dữ liệu trong nhóm kết nối để xử lý logic vòng đời'''

    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection
        self.usage = 0
        self.last = self.created = time.time()

    def using(self):
        '''Sử dụng phương thức này khi kết nối được gọi, số lần sử dụng +1'''
        self.usage += 1
        self.last = time.time()
        return self

    def reset(self):
        '''Đặt lại trạng thái gói kết nối'''
        self.usage = self.last = self.created = 0

    def __enter__(self):
        return self.connection

    def __exit__(self, *exc_info):
        self.pool.release(self)


class ConnectionPool(object):
    '''Lớp nhóm kết nối, có thể được sử dụng cho pymysql/memcache/redis/... 

    Ví dụ kết nối redis có thể được gọi:
        pool = ConnectionPool(create=redis.Redis)

    Kết nối được gọi thông qua hàm lambda để có thể truyền parameter list vào hàm create connect：
        pool = ConnectionPool(create=lambda: redis.Redis(host='127.0.0.1'))

    Có thể sử dụng functools.partial
        from functools import partial
        pool = ConnectionPool(create=partial(redis.Redis, host='127.0.0.1'))
    '''

    __wrappers = {}

    def __init__(self, create, close=None, max_size=10, max_usage=0, ttl=0, idle=60, block=True):
        '''Tham số khởi tạo

            create:    must be a callback function, creates connection
            close:     optional callback to close connection
            max_size:  the maximum number of connections, 0 is no limit (not recommended)
            max_usage: the number of times the connection can be used,
                       after this number of times, the connection will be released/closed
            ttl:       connection use time limit (seconds), when reached, the connection
                       will be released/closed
            idle:      connection idle time (seconds), when reached, the connection
                       will be released/closed
            block:     When the number of connections is full,
                       whether to block and wait for the connection to be released.
                       Enter False to throw an exception when the connection pool is full
        '''
        if not hasattr(create, '__call__'):
            raise ValueError('"create" argument is not callable')

        if close is not None and not hasattr(close, '__call__'):
            raise ValueError('"close" argument is not callable')

        self._create = create
        self._close = close
        self._max_size = int(max_size)
        self._max_usage = int(max_usage)
        self._ttl = int(ttl)
        self._idle = int(idle)
        self._block = bool(block)
        self._lock = threading.Condition()
        self._pool = queue.Queue()
        self._size = 0

    def item(self):
        '''Sử dụng cấu trúc with ... as ... của python

            pool = ConnectionPool(create=redis.Redis)
            with pool.item() as redis:
                redis.set('foo', 'bar)
        '''
        self._lock.acquire()

        try:
            while (self._max_size and self._pool.empty() and self._size >= self._max_size):
                if not self._block:
                    raise TooManyConnections('Too many connections')

                self._lock.wait()  # Đang đợi kết nối nhàn rỗi

            try:
                wrapped = self._pool.get_nowait()  # Lấy ra 1 kết nối từ pool mà không chờ đợi tài nguyên
                if self._idle and (wrapped.last + self._idle) < time.time():
                    self._destroy(wrapped)
                    raise IdleExceeded('Idle exceeds %d secs' % self._idle)
            except (queue.Empty, IdleExceeded):
                wrapped = self._wrapper(self._create())  # Tạo một kết nối mới
                self._size += 1
        finally:
            self._lock.release()

        return wrapped.using()

    def release(self, conn):
        '''Giải phóng kết nối, để kết nối quay trở lại pool

        Khi sử dụng kết nối vượt quá giới hạn / vượt quá thời gian giới hạn, kết nối sẽ bị hủy
        '''
        self._lock.acquire()
        wrapped = self._wrapper(conn)

        try:
            self._test(wrapped)
        except Expired:
            self._destroy(wrapped)
        else:
            self._pool.put_nowait(wrapped)
            self._lock.notifyAll()  # Thông báo cho các thread khác rằng có kết nối không hoạt động, sẵn sàng cho thread khác sử dụng
        finally:
            self._lock.release()

    def _destroy(self, wrapped):
        '''Hủy kết nối'''
        if self._close:
            self._close(wrapped.connection)

        self._unwrapper(wrapped)
        self._size -= 1

    def _wrapper(self, conn):
        '''Sử dụng id để xác định kết nối'''
        if isinstance(conn, WrapperConnection):
            return conn

        _id = id(conn)

        if _id not in self.__wrappers:
            self.__wrappers[_id] = WrapperConnection(self, conn)

        return self.__wrappers[_id]

    def _unwrapper(self, wrapped):
        '''Giải phóng kết nối'''
        if not isinstance(wrapped, WrapperConnection):
            return

        _id = id(wrapped.connection)
        wrapped.reset()
        del wrapped

        if _id in self.__wrappers:
            del self.__wrappers[_id]

    def _test(self, wrapped):
        '''Kiểm tra tính khả dụng của kết nối và ném ra exception Expired khi nó không khả dụng'''
        if self._max_usage and wrapped.usage >= self._max_usage:
            raise UsageExceeded('Usage exceeds %d times' % self._max_usage)

        if self._ttl and (wrapped.created + self._ttl) < time.time():
            raise TtlExceeded('TTL exceeds %d secs' % self._ttl)
