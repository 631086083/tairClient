from unittest import TestCase, main

import redis

from tairClient.client import Client
from datetime import datetime, timedelta
from time import time, sleep

client = None
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379


class TestTairClient(TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        global client
        client = Client(host=REDIS_HOST,port=REDIS_PORT)
        client.flushall()

    def setUp(self):
        client.flushall()

    def test_exhset_nx(self):
        self.assertEqual(1, client.exhset('key', 'a', '1', nx=True))
        self.assertEqual(-1, client.exhset('key', 'a', '1', nx=True))
        self.assertEqual(b'1', client.exhget('key', 'a'))

    def test_exhset_xx(self):
        self.assertEqual(-1, client.exhset('key', 'a', '2', xx=True))
        self.assertEqual(1, client.exhset('key', 'a', '2'))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertEqual(0, client.exhset('key', 'a', '3', xx=True))
        self.assertEqual(b'3', client.exhget('key', 'a'))
        self.assertEqual(-1, client.exhset('key1', 'a', '3', xx=True))

    def test_exhset_px(self):
        self.assertEqual(1, client.exhset('key', 'a', 1, px=10000))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_px_timedelta(self):
        pexpire_timedelta = timedelta(milliseconds=10000)
        self.assertEqual(1, client.exhset('key', 'a', 1, px=pexpire_timedelta))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_ex(self):
        self.assertEqual(1, client.exhset('key', 'a', 1, ex=10))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_ex_timedelta(self):
        expire_timedelta = timedelta(seconds=10)
        self.assertEqual(1, client.exhset('key', 'a', 1, ex=expire_timedelta))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_pxat(self):
        pexpire_at = int(time() * 1000) + 10000
        self.assertEqual(1, client.exhset('key', 'a', 1, pxat=pexpire_at))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_pxat_timedelta(self):
        pexpire_at_timedelta = datetime.now() + timedelta(milliseconds=10000)
        self.assertEqual(1, client.exhset('key', 'a', 1, pxat=pexpire_at_timedelta))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_exat(self):
        expire_at = int(time()) + 10
        self.assertEqual(1, client.exhset('key', 'a', 1, exat=expire_at))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_exat_timedelta(self):
        expire_at_timedelta = datetime.now() + timedelta(seconds=10)
        self.assertEqual(1, client.exhset('key', 'a', 1, exat=expire_at_timedelta))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhset_ver(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(0, client.exhset('key', 'a', 2, ver=1))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertEqual(2, client.exhver('key', 'a'))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhset('key', 'a', 2, ver=1)
        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhset('key', 'a', 2, ver=3)

        self.assertEqual(1, client.exhset('key', 'b', 2, ver=6))
        self.assertEqual(b'2', client.exhget('key', 'b'))
        self.assertEqual(1, client.exhver('key', 'b'))

        self.assertEqual(1, client.exhset('key1', 'b', 2, ver=6))
        self.assertEqual(b'2', client.exhget('key1', 'b'))
        self.assertEqual(1, client.exhver('key1', 'b'))

    def test_exhset_abs(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(1, client.exhver('key', 'a'))
        self.assertEqual(0, client.exhset('key', 'a', 2, abs=6))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertEqual(6, client.exhver('key', 'a'))
        self.assertEqual(1, client.exhset('key1', 'b', 'b', abs=6))
        self.assertEqual(b'b', client.exhget('key1', 'b'))
        self.assertEqual(6, client.exhver('key1', 'b'))

    # def test_exhset_noactive(self):
    #     pexpire_at_timedelta = datetime.now() + timedelta(milliseconds=100)
    #     self.assertEqual(1, client.exhset('key', 'a', 1, pxat=pexpire_at_timedelta, noactive=True))
    #     sleep(200)
    #     self.assertEqual(1, client.exhlen('key', True))

    def test_exhmset_and_exhmget(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(b'2', client.exhget('key', 'b'))
        self.assertEqual(b'3', client.exhget('key', 'c'))
        self.assertEqual([b'1', b'2', b'3'], client.exhmget('key', ['a', 'b', 'c']))
        self.assertEqual([b'1', b'2', b'3'], client.exhmget('key', 'a', 'b', 'c'))

    def test_exhpexpireat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhpexpireat = int(time() * 1000) + 10000
        self.assertEqual(1, client.exhpexpireat('key', 'a', pxat=exhpexpireat))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhpexpireat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhpexpireat_timedelta = datetime.now() + timedelta(microseconds=10000)
        self.assertEqual(1, client.exhpexpireat('key', 'a', pxat=exhpexpireat_timedelta))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhpexpireat_ver(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhpexpireat = int(time() * 1000) + 10000
        self.assertEqual(1, client.exhpexpireat('key', 'a', pxat=exhpexpireat, ver=1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhpexpireat('key', 'a', pxat=exhpexpireat, ver=2)

        self.assertEqual(0, client.exhpexpireat('key', 'b', pxat=exhpexpireat, ver=2))
        self.assertEqual(0, client.exhpexpireat('key1', 'b', pxat=exhpexpireat, ver=2))

    def test_exhpexpireat_abs(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhpexpireat = int(time() * 1000) + 10000
        self.assertEqual(1, client.exhpexpireat('key', 'a', pxat=exhpexpireat, abs=6))
        self.assertEqual(6, client.exhver('key', 'a'))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        self.assertEqual(0, client.exhpexpireat('key', 'b', pxat=exhpexpireat, abs=2))
        self.assertEqual(0, client.exhpexpireat('key1', 'b', pxat=exhpexpireat, abs=2))

    def test_exhexpireat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhexpireat = int(time()) + 10
        client.exhexpireat('key', 'a', exat=exhexpireat)
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhexpireat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhexpireat = datetime.now() + timedelta(seconds=10)
        client.exhexpireat('key', 'a', exat=exhexpireat)
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhexpireat_ver(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhexpireat = int(time()) + 10
        self.assertEqual(1, client.exhexpireat('key', 'a', exat=exhexpireat, ver=1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhexpireat('key', 'a', exat=exhexpireat, ver=2)

        self.assertEqual(0, client.exhexpireat('key', 'b', exat=exhexpireat, ver=2))
        self.assertEqual(0, client.exhexpireat('key1', 'b', exat=exhexpireat, ver=2))

    def test_exhexpireat_abs(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhexpireat = int(time()) + 10
        self.assertEqual(1, client.exhexpireat('key', 'a', exat=exhexpireat, abs=6))
        self.assertEqual(6, client.exhver('key', 'a'))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        self.assertEqual(0, client.exhexpireat('key', 'b', exat=exhexpireat, abs=2))
        self.assertEqual(0, client.exhexpireat('key1', 'b', exat=exhexpireat, abs=2))

    def test_exhexpire(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        self.assertEqual(1, client.exhexpire('key', 'a', ex=10))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhexpire_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        exhexpire = timedelta(seconds=10)
        self.assertEqual(1, client.exhexpire('key', 'a', ex=exhexpire))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhexpire_ver(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        self.assertEqual(1, client.exhexpire('key', 'a', ex=10, ver=1))

        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        try:
            self.assertEqual(1, client.exhexpire('key', 'a', ex=10, ver=2))
        except Exception as e:
            self.assertEqual("update version is stale", str(e))

        self.assertEqual(0, client.exhexpire('key', 'b', ex=10, ver=2))
        self.assertEqual(0, client.exhexpire('key1', 'b', ex=10, ver=2))

    def test_exhexpire_abs(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        self.assertEqual(1, client.exhexpire('key', 'a', ex=10, abs=6))

        self.assertEqual(6, client.exhver('key', 'a'))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        self.assertEqual(0, client.exhexpire('key', 'b', ex=10, abs=2))
        self.assertEqual(0, client.exhexpire('key1', 'b', ex=10, abs=2))

    def test_exhpexpire(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        client.exhpexpire('key', 'a', px=10000)

        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhpexpire_timedelta(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        exhpexpire = timedelta(milliseconds=10000)
        client.exhpexpire('key', 'a', px=exhpexpire)

        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhpexpire_ver(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        self.assertEqual(1, client.exhpexpire('key', 'a', px=10000, ver=1))

        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        try:
            self.assertEqual(1, client.exhpexpire('key', 'a', px=10000, ver=2))
        except Exception as e:
            self.assertEqual("update version is stale", str(e))

        self.assertEqual(0, client.exhpexpire('key', 'b', px=10000, ver=2))
        self.assertEqual(0, client.exhpexpire('key1', 'b', px=10000, ver=2))

    def test_exhpexpire_abs(self):

        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        self.assertEqual(1, client.exhpexpire('key', 'a', px=10000, abs=6))

        self.assertEqual(6, client.exhver('key', 'a'))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

        self.assertEqual(0, client.exhpexpire('key', 'b', px=10000, abs=2))
        self.assertEqual(0, client.exhpexpire('key1', 'b', px=10000, abs=2))

    def test_exhsetver(self):
        self.assertEqual(1, client.exhset('key', 'a', '1'))
        self.assertEqual(1, client.exhsetver('key', 'a', 6))
        self.assertEqual(6, client.exhver('key', 'a'))

        self.assertEqual(0, client.exhsetver('key', 'b', 6))
        self.assertEqual(0, client.exhsetver('key1', 'b', 6))

    def test_exhgetwithver(self):
        self.assertEqual(1, client.exhset('key', 'a', '1'))
        self.assertEqual([b'1', 1], client.exhgetwithver('key', 'a'))
        self.assertEqual(1, client.exhsetver('key', 'a', 6))
        self.assertEqual([b'1', 6], client.exhgetwithver('key', 'a'))

    def test_exhmgetwithver(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual([[b'1', 1], [b'2', 1], [b'3', 1]], client.exhmgetwithver('key', 'a', 'b', 'c'))

    def test_exhdel(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual(2, client.exhdel('key', 'a', 'b'))
        self.assertEqual(0, client.exhdel('key', 'a', 'b'))
        self.assertEqual(0, client.exhdel('key1', 'a', 'b'))

    def test_exhlen(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual(3, client.exhlen('key'))
        self.assertEqual(2, client.exhdel('key', 'a', 'b'))
        self.assertEqual(1, client.exhlen('key'))

    def test_exhexists(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual(1, client.exhexists('key', 'a'))
        self.assertEqual(0, client.exhexists('key', 'd'))
        self.assertEqual(0, client.exhexists('key1', 'a'))

    def test_exhstrlen(self):
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 22, 'c': 333}))
        self.assertEqual(1, client.exhstrlen('key', 'a'))
        self.assertEqual(2, client.exhstrlen('key', 'b'))
        self.assertEqual(3, client.exhstrlen('key', 'c'))
        self.assertEqual(0, client.exhstrlen('key', 'd'))
        self.assertEqual(0, client.exhstrlen('key1', 'a'))

    def test_exhkeys(self):
        self.assertEqual([], client.exhkeys('key'))
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual([b'c', b'b', b'a'], client.exhkeys('key'))

    def test_exhvals(self):
        self.assertEqual([], client.exhvals('key'))
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual([b'3', b'2', b'1'], client.exhvals('key'))

    def test_exhgetall(self):
        self.assertEqual([], client.exhgetall('key'))
        self.assertEqual(b'OK', client.exhmset('key', mapping={'a': 1, 'b': 2, 'c': 3}))
        self.assertEqual([b'c', b'3', b'b', b'2', b'a', b'1'], client.exhgetall('key'))

    "----------------------------------"

    def test_exhincrby(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(2, client.exhincrby('key', 'a', 1))
        self.assertEqual(3, client.exhincrby('key', 'a', 1))

        self.assertEqual(1, client.exhincrby('key1', 'a', 1))
        self.assertEqual(1, client.exhincrby('key', 'b', 1))

        self.assertEqual(1, client.exhset('key', 'k', 'v'))
        with self.assertRaisesRegex(redis.exceptions.ResponseError, "value is not an integer"):
            client.exhincrby('key', 'k', 1)

    def test_exhincrby_ex(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        self.assertEqual(2, client.exhincrby('key', 'a', 1, ex=10))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_ex_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        ex = timedelta(seconds=10)
        self.assertEqual(2, client.exhincrby('key', 'a', 1, ex=ex))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_px(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        self.assertEqual(2, client.exhincrby('key', 'a', 1, px=10000))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_px_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        px = timedelta(seconds=10)
        self.assertEqual(2, client.exhincrby('key', 'a', 1, px=px))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_exat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        exat = int(time()) + 10
        self.assertEqual(2, client.exhincrby('key', 'a', 1, exat=exat))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_exat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        exat = timedelta(seconds=10) + datetime.now()
        self.assertEqual(2, client.exhincrby('key', 'a', 1, exat=exat))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_pxat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        pxat = int(time()) * 1000 + 10000
        self.assertEqual(2, client.exhincrby('key', 'a', 1, pxat=pxat))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrby_pxat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        pxat = timedelta(milliseconds=10000) + datetime.now()
        self.assertEqual(2, client.exhincrby('key', 'a', 1, pxat=pxat))
        self.assertEqual(b'2', client.exhget('key', 'a'))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    "------------"

    def test_exhincrby_ver(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertEqual(3, client.exhincrby('key', 'a', 2, ver=1))
        self.assertEqual(b'3', client.exhget('key', 'a'))
        self.assertEqual(2, client.exhver('key', 'a'))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhincrby('key', 'a', 2, ver=1)
        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhincrby('key', 'a', 2, ver=3)

        self.assertEqual(2, client.exhincrby('key', 'b', 2, ver=6))
        self.assertEqual(b'2', client.exhget('key', 'b'))
        self.assertEqual(1, client.exhver('key', 'b'))

        self.assertEqual(2, client.exhincrby('key1', 'b', 2, ver=6))
        self.assertEqual(b'2', client.exhget('key1', 'b'))
        self.assertEqual(1, client.exhver('key1', 'b'))

    def test_exhincrby_abs(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(1, client.exhver('key', 'a'))

        self.assertEqual(3, client.exhincrby('key', 'a', 2, abs=6))
        self.assertEqual(b'3', client.exhget('key', 'a'))
        self.assertEqual(6, client.exhver('key', 'a'))

        self.assertEqual(1, client.exhincrby('key1', 'b', 1, abs=6))
        self.assertEqual(b'1', client.exhget('key1', 'b'))
        self.assertEqual(6, client.exhver('key1', 'b'))

    def test_exhincrby_minval(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertEqual(3, client.exhincrby('key', 'a', 2, minval=3))
        self.assertEqual(5, client.exhincrby('key', 'a', 2, minval=3))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "increment or decrement would overflow"):
            client.exhincrby('key', 'a', 2, minval=10)

    def test_exhincrby_maxval(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertEqual(3, client.exhincrby('key', 'a', 2, maxval=10))
        self.assertEqual(5, client.exhincrby('key', 'a', 2, maxval=10))
        self.assertEqual(7, client.exhincrby('key', 'a', 2, maxval=10))
        self.assertEqual(9, client.exhincrby('key', 'a', 2, maxval=10))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "increment or decrement would overflow"):
            client.exhincrby('key', 'a', 2, maxval=10)

    def test_exhincrbyfloat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertAlmostEqual(2.0, float(client.exhincrbyfloat('key', 'a', 1.0)))
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 1.0)))

        self.assertAlmostEqual(1.0, float(client.exhincrbyfloat('key1', 'a', 1.0)))
        self.assertAlmostEqual(1.0, float(client.exhincrbyfloat('key', 'b', 1.0)))

        self.assertEqual(1, client.exhset('key', 'k', 'v'))
        with self.assertRaisesRegex(redis.exceptions.ResponseError, "value is not an float"):
            client.exhincrbyfloat('key', 'k', 1)

    def test_exhincrbyfloat_ex(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, ex=10)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_ex_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        ex = timedelta(seconds=10)
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, ex=ex)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_px(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))

        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, px=10000)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_px_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        px = timedelta(seconds=10)
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, px=px)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_exat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        exat = int(time()) + 10
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, exat=exat)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_exat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        exat = timedelta(seconds=10) + datetime.now()
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, exat=exat)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_pxat(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        pxat = int(time()) * 1000 + 10000
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, pxat=pxat)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_pxat_timedelta(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(-1, client.exhpttl('key', 'a'))
        pxat = timedelta(seconds=10) + datetime.now()
        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, pxat=pxat)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertLessEqual(client.exhpttl('key', 'a'), 10000)
        self.assertLessEqual(0, client.exhpttl('key', 'a'))
        self.assertLessEqual(client.exhttl('key', 'a'), 10)
        self.assertLessEqual(0, client.exhttl('key', 'a'))

    def test_exhincrbyfloat_ver(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, ver=1)))
        self.assertAlmostEqual(3.0, float(client.exhget('key', 'a')))
        self.assertEqual(2, client.exhver('key', 'a'))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhincrbyfloat('key', 'a', 2, ver=1)
        with self.assertRaisesRegex(redis.exceptions.ResponseError, "update version is stale"):
            client.exhincrbyfloat('key', 'a', 2, ver=3)

        self.assertAlmostEqual(2.1, float(client.exhincrbyfloat('key', 'b', 2.1, ver=6)))
        self.assertAlmostEqual(2.1, float(client.exhget('key', 'b')))
        self.assertEqual(1, client.exhver('key', 'b'))

        self.assertAlmostEqual(2.9, float(client.exhincrbyfloat('key1', 'b', 2.9, ver=6)))
        self.assertAlmostEqual(2.9, float(client.exhget('key1', 'b')))
        self.assertEqual(1, client.exhver('key1', 'b'))

    def test_exhincrby_abs(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))
        self.assertEqual(1, client.exhver('key', 'a'))

        self.assertAlmostEqual(3.5, float(client.exhincrbyfloat('key', 'a', 2.5, abs=6)))
        self.assertAlmostEqual(3.5, float(client.exhget('key', 'a')))
        self.assertEqual(6, client.exhver('key', 'a'))

        self.assertAlmostEqual(1.9, float(client.exhincrbyfloat('key1', 'b', 1.9, abs=6)))
        self.assertAlmostEqual(1.9, float(client.exhget('key1', 'b')))
        self.assertEqual(6, client.exhver('key1', 'b'))

    def test_exhincrbyfloat_minval(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, minval=3)))
        self.assertAlmostEqual(5.0, float(client.exhincrbyfloat('key', 'a', 2.0, minval=3)))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "increment or decrement would overflow"):
            client.exhincrby('key', 'a', 2, minval=10)

    def test_exhincrby_maxval(self):
        self.assertEqual(1, client.exhset('key', 'a', 1))
        self.assertEqual(b'1', client.exhget('key', 'a'))

        self.assertAlmostEqual(3.0, float(client.exhincrbyfloat('key', 'a', 2.0, maxval=10)))
        self.assertAlmostEqual(5.0, float(client.exhincrbyfloat('key', 'a', 2.0, maxval=10)))
        self.assertAlmostEqual(7.0, float(client.exhincrbyfloat('key', 'a', 2.0, maxval=10)))
        self.assertAlmostEqual(9.0, float(client.exhincrbyfloat('key', 'a', 2.0, maxval=10)))

        with self.assertRaisesRegex(redis.exceptions.ResponseError, "increment or decrement would overflow"):
            client.exhincrby('key', 'a', 2, maxval=10)


if __name__ == "__main__":
    main()
